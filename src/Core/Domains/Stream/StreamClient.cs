using System.Runtime.CompilerServices;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamClient : IStreamClient, IDisposable
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    private readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    private readonly Func<Action, IDisposable>? _registerOnDisconnect;
    private readonly Func<Func<CancellationToken, ValueTask>, bool>? _dispatchAsyncHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, StreamSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal StreamClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterBorrowedNotificationHandler,
            connection.OnDisconnect,
            connection.TryDispatchAsyncHandler,
            (operation, messageType, payload, cancellationToken) =>
                connection.ExecuteWithRetryAsync(
                    operation,
                    innerToken => connection.RequestAsync(messageType, payload, innerToken),
                    cancellationToken))
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public StreamClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
    }

    internal StreamClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null,
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retryRequest = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _registerOnDisconnect = registerOnDisconnect;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _retryRequest = retryRequest;
    }

    public async Task<IStreamSession> BeginAsync(string route, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default)
    {
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        if (ingestMetadata.HasValue && ingestMetadata.Value.Length > 0)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)ingestMetadata.Value.Length);
            writer.WriteBytes(ingestMetadata.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }

        var response = await _request(MessageTypes.StreamBegin, writer.WrittenMemory, ct).ConfigureAwait(false);
        return new StreamSession(_request, StreamWireHelpers.ReadExpectedSessionId(response, "BEGIN", "MISSING_SESSION_ID"), _registerOnDisconnect);
    }

    public async IAsyncEnumerable<StreamRecord> ReadAsync(
        string route,
        ulong startOffset,
        ulong limit = 100,
        StreamFilterSet? filter = null,
        ulong? maxBytes = null,
        ulong? cursorFingerprint = null, ulong? capturedWatermark = null, string? resumeRealm = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var page = await ReadPageAsync(route, startOffset, limit, filter, maxBytes, cursorFingerprint, capturedWatermark, resumeRealm, ct).ConfigureAwait(false);
        foreach (var item in page.Items)
        {
            if (item.Kind == StreamReadItemKind.Event && item.Record is not null)
            {
                yield return item.Record;
            }
        }
    }

    public async Task<StreamReadPage> ReadPageAsync(
        string route,
        ulong startOffset,
        ulong limit = 100,
        StreamFilterSet? filter = null,
        ulong? maxBytes = null,
        ulong? cursorFingerprint = null, ulong? capturedWatermark = null, string? resumeRealm = null,
        CancellationToken ct = default)
    {
        ValidateStreamSelector(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(startOffset);
        writer.WriteU64(limit);
        writer.WriteU8((byte)(maxBytes.HasValue ? 1 : 0));
        if (maxBytes.HasValue)
        {
            writer.WriteU64(maxBytes.Value);
        }

        var filterBytes = StreamWireHelpers.EncodeStreamFilterSet(filter);
        writer.WriteU8((byte)(filterBytes.Length > 0 ? 1 : 0));
        if (filterBytes.Length > 0)
        {
            writer.WriteU32((uint)filterBytes.Length);
            writer.WriteBytes(filterBytes);
        }

        if (resumeRealm is not null)
        {
            writer.WriteU8(1); writer.WriteString(resumeRealm);
        }
        else
        {
            writer.WriteU8((byte)(cursorFingerprint.HasValue ? 1 : 0)); if (cursorFingerprint.HasValue) writer.WriteU64(cursorFingerprint.Value);
            writer.WriteU8((byte)(capturedWatermark.HasValue ? 1 : 0)); if (capturedWatermark.HasValue) writer.WriteU64(capturedWatermark.Value);
        }

        var response = await RequestWithRetryAsync(
            new RetryOperation("stream", "read", RetryClass.ReplayableRead),
            MessageTypes.StreamRead,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "READ");
        return data.IsEmpty ? new StreamReadPage(Array.Empty<StreamReadItem>(), new StreamReadCursor(0, null, null, null, null, null, null, false)) : StreamWireHelpers.ReadReadPage(data, "READ", route);
    }

    public async Task<StreamRecord?> PeekAsync(string route, CancellationToken ct = default)
    {
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await RequestWithRetryAsync(
            new RetryOperation("stream", "last", RetryClass.ReplayableRead),
            MessageTypes.StreamLast,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "LAST");
        if (data.IsEmpty)
        {
            return null;
        }

        return StreamWireHelpers.ReadRoutedRecord(data, "LAST");
    }

    public async Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default)
    {
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await RequestWithRetryAsync(
            new RetryOperation("stream", "metadata", RetryClass.ReplayableRead),
            MessageTypes.StreamGetMetadata,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "METADATA");

        if (data.IsEmpty)
        {
            return new StreamMetadata(0, 0, 0);
        }

        var inner = new BinaryBufferReader(data);
        var firstOffset = StreamWireHelpers.ReadOptionalU64(inner, "METADATA", "first offset") ?? 0;
        var lastOffset = StreamWireHelpers.ReadOptionalU64(inner, "METADATA", "last offset") ?? 0;
        if (inner.RemainingBytes < 24)
        {
            throw new StreamException("METADATA response missing metadata payload", "METADATA_INVALID_RESPONSE");
        }

        var recordCount = inner.ReadU64();
        _ = inner.ReadU64();
        _ = inner.ReadU64();
        _ = StreamWireHelpers.ReadOptionalU64(inner, "METADATA", "ttl seconds");

        if (inner.RemainingBytes < 16)
        {
            throw new StreamException("METADATA response missing metadata payload", "METADATA_INVALID_RESPONSE");
        }

        _ = inner.ReadU64();
        _ = inner.ReadU64();

        if (!inner.IsEof)
        {
            throw new StreamException("METADATA response has trailing bytes", "METADATA_INVALID_RESPONSE");
        }

        return new StreamMetadata(firstOffset, lastOffset, recordCount);
    }

    public async Task<StreamSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default)
    {
        var buffer = new AsyncSubscriptionBuffer<StreamCommitEvent>(pattern);
        var registration = await SubscribeAsync(pattern, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, ct).ConfigureAwait(false);
        return new StreamSubscription(pattern, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            buffer.Complete();
            await registration.UnsubscribeAsync(token).ConfigureAwait(false);
        });
    }

    internal async Task<StreamSubscription> SubscribeAsync(
        string pattern,
        Func<StreamCommitEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsStreamSelector(pattern))
        {
            throw new StreamException($"pattern '{pattern}' must use whole-segment wildcards and match a three-segment stream route", "INVALID_ROUTE");
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<StreamCommitEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        SubscriptionRegistration<StreamCommitEvent>? registration = null;
        var handleId = Interlocked.Increment(ref _nextHandleId);
        var gateAcquired = false;

        try
        {
            registration = new SubscriptionRegistration<StreamCommitEvent>(channel);
            await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
            gateAcquired = true;

            StreamSubscription? existingHandle = null;
            lock (_gate)
            {
                if (_subscriptionsByPattern.TryGetValue(pattern, out var existingSubscription))
                {
                    existingSubscription.Registrations[handleId] = registration;
                    existingHandle = CreateSubscription(pattern, handleId);
                }
            }

            if (existingHandle is not null)
            {
                SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                registration = null;
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new StreamSubscriptionState(subscriptionId);
            lock (_gate)
            {
                subscription.Registrations[handleId] = registration;
                _subscriptionsByPattern[pattern] = subscription;
                _patternsBySubscriptionId[subscriptionId] = pattern;
            }

            var handle = CreateSubscription(pattern, handleId);
            SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
            registration = null;
            return handle;
        }
        finally
        {
            if (gateAcquired)
            {
                _subscriptionGate.Release();
            }

            registration?.Dispose();
        }
    }

    private StreamSubscription CreateSubscription(string pattern, long handleId)
    {
        return new StreamSubscription(
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken));
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        return StreamWireHelpers.ReadExpectedSessionId(response, "SUBSCRIBE", "MISSING_SUB_ID");
    }

    private async Task UnsubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        StreamWireHelpers.EnsureSuccessStatusOnly(response, "UNSUBSCRIBE");
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<StreamCommitEvent>? registration = null;
        bool shouldUnsubscribe = false;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_gate)
            {
                if (!_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                if (!subscription.Registrations.Remove(handleId, out registration))
                {
                    return;
                }

                if (subscription.Registrations.Count == 0)
                {
                    _subscriptionsByPattern.Remove(pattern);
                    _patternsBySubscriptionId.Remove(subscription.SubscriptionId);
                    shouldUnsubscribe = true;
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
            registration?.Dispose();
        }

        if (shouldUnsubscribe)
        {
            await UnsubscribeWireAsync(pattern, ct).ConfigureAwait(false);
        }
    }

    private void EnsureNotificationHandlerInitialized()
    {
        if (_notificationHandlerInitialized)
        {
            return;
        }

        if (_registerNotificationHandler is null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        _notificationHandlerInitialized = true;
        _notificationRegistration = _registerNotificationHandler(MessageTypes.StreamNotify, HandleNotification);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed broker notifications are dropped without disrupting the receive loop.")]
    private void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var notifyReader = new BinaryBufferReader(payload);
            var subscriptionId = notifyReader.ReadU64();
            var route = notifyReader.ReadString();
            var bodyLength = notifyReader.ReadU32();
            var body = notifyReader.ReadSpan((int)bodyLength);
            var notification = new StreamCommitEvent(route, StreamWireHelpers.TryParseCommitOffset(body));

            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                foreach (var registration in subscription.Registrations.Values)
                {
                    registration.Channel.Writer.TryWrite(notification);
                }
            }
        }
        catch
        {
        }
    }

    private async ValueTask HandleReconnect(CancellationToken cancellationToken)
    {
        await RestoreSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateExactStreamRoute(string route)
    {
        if (RouteValidation.TryValidateFixedRoute(route, "stream", 3, out var failure))
        {
            return;
        }

        ThrowInvalidExactRoute(route, failure);
    }

    private static void ValidateStreamSelector(string route)
    {
        if (RouteValidation.IsStreamSelector(route))
        {
            return;
        }

        throw new StreamException($"stream selector '{route}' must be realm/area/resource, realm/area/*, realm/*/*, or stream://**", "INVALID_ROUTE");
    }

    private static void ThrowInvalidExactRoute(string route, RouteValidationFailure failure)
    {
        var message = failure switch
        {
            RouteValidationFailure.InvalidScheme => $"stream route '{route}' must start with stream://",
            RouteValidationFailure.EmptySegment => $"stream route '{route}' segments must be non-empty",
            _ => $"stream route '{route}' must be stream://{{realm}}/{{area}}/{{resource}}",
        };

        throw new StreamException(message, "INVALID_ROUTE");
    }

    private async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<(string Pattern, StreamSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByPattern.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Pattern, StreamSubscriptionState Subscription)>(_subscriptionsByPattern.Count);
                foreach (var entry in _subscriptionsByPattern)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, StreamSubscriptionState>(StringComparer.Ordinal);
            var restoredPatternsById = new Dictionary<ulong, string>();

            foreach (var entry in snapshot)
            {
                var subscriptionId = await SubscribeWireAsync(entry.Pattern, cancellationToken).ConfigureAwait(false);
                restoredSubscriptions[entry.Pattern] = entry.Subscription.Clone(subscriptionId);
                restoredPatternsById[subscriptionId] = entry.Pattern;
            }

            lock (_gate)
            {
                _subscriptionsByPattern.Clear();
                _patternsBySubscriptionId.Clear();

                foreach (var entry in restoredSubscriptions)
                {
                    _subscriptionsByPattern[entry.Key] = entry.Value;
                }

                foreach (var entry in restoredPatternsById)
                {
                    _patternsBySubscriptionId[entry.Key] = entry.Value;
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    private ValueTask<ReadOnlyMemory<byte>> RequestWithRetryAsync(
        RetryOperation operation,
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        if (_retryRequest is null)
        {
            return _request(messageType, payload, cancellationToken);
        }

        return _retryRequest(operation, messageType, payload, cancellationToken);
    }

    public void Dispose()
    {
        _notificationRegistration?.Dispose();
        _reconnectRegistration?.Dispose();
        lock (_gate)
        {
            foreach (var subscription in _subscriptionsByPattern.Values)
            {
                foreach (var registration in subscription.Registrations.Values)
                {
                    registration.Dispose();
                }
            }

            _subscriptionsByPattern.Clear();
            _patternsBySubscriptionId.Clear();
        }

        _subscriptionGate.Dispose();
    }

    private sealed class StreamSubscriptionState
    {
        public StreamSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; init; }

        public Dictionary<long, SubscriptionRegistration<StreamCommitEvent>> Registrations { get; } = new();

        public StreamSubscriptionState Clone() => Clone(SubscriptionId);

        public StreamSubscriptionState Clone(ulong subscriptionId)
        {
            var clone = new StreamSubscriptionState(subscriptionId);
            foreach (var entry in Registrations)
            {
                clone.Registrations.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }

}
