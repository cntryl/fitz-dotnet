using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamClient : IStreamClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, StreamSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal StreamClient(FitzConnection connection)
        : this(connection.RequestAsync, connection.RegisterNotificationHandler)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public StreamClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
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

        var response = await _request(MessageTypes.StreamBegin, writer.Build(), ct).ConfigureAwait(false);
        return new StreamSession(_request, StreamWireHelpers.ReadExpectedSessionId(response, "BEGIN", "MISSING_SESSION_ID"));
    }

    public async IAsyncEnumerable<StreamRecord> ReadAsync(
        string route,
        ulong startOffset,
        ulong limit = 100,
        ulong? maxBytes = null,
        [EnumeratorCancellation] CancellationToken ct = default)
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

        var response = await _request(MessageTypes.StreamRead, writer.Build(), ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "READ");

        if (data.IsEmpty)
        {
            yield break;
        }

        var records = ParseReadRecords(data);
        foreach (var record in records)
        {
            yield return record;
        }
    }

    public async Task<StreamRecord?> PeekAsync(string route, CancellationToken ct = default)
    {
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.StreamLast, writer.Build(), ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "LAST");
        if (data.IsEmpty)
        {
            return null;
        }

        return StreamWireHelpers.ReadRecord(data, "LAST");
    }

    public async Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default)
    {
        ValidateExactStreamRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.StreamGetMetadata, writer.Build(), ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "METADATA");

        if (data.IsEmpty)
        {
            return new StreamMetadata(0, 0, 0);
        }

        var inner = new BinaryBufferReader(data);
        if (inner.RemainingBytes < 24)
        {
            throw new StreamException("METADATA response missing metadata payload", "METADATA_INVALID_RESPONSE");
        }

        return new StreamMetadata(inner.ReadU64(), inner.ReadU64(), inner.ReadU64());
    }

    public async Task<StreamSubscription> SubscribeAsync(
        string pattern,
        Func<StreamCommitEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ValidateStreamSelector(pattern);

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
        var registration = new SubscriptionRegistration<StreamCommitEvent>(channel);
        var handleId = Interlocked.Increment(ref _nextHandleId);

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_subscriptionsByPattern.TryGetValue(pattern, out var existingSubscription))
            {
                existingSubscription.Registrations[handleId] = registration;
                var existingHandle = CreateSubscription(pattern, handleId, existingSubscription.SubscriptionId);
                SubscriptionPump.Start(registration, handler);
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new StreamSubscriptionState(subscriptionId);
            subscription.Registrations[handleId] = registration;
            _subscriptionsByPattern[pattern] = subscription;
            _patternsBySubscriptionId[subscriptionId] = pattern;

            var handle = CreateSubscription(pattern, handleId, subscriptionId);
            SubscriptionPump.Start(registration, handler);
            return handle;
        }
        catch
        {
            registration.Dispose();
            throw;
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    private StreamSubscription CreateSubscription(string pattern, long handleId, ulong subscriptionId)
    {
        return new StreamSubscription(
            subscriptionId,
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken));
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamSubscribe, writer.Build(), ct).ConfigureAwait(false);
        return StreamWireHelpers.ReadExpectedSessionId(response, "SUBSCRIBE", "MISSING_SUB_ID");
    }

    private async Task UnsubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamUnsubscribe, writer.Build(), ct).ConfigureAwait(false);
        StreamWireHelpers.EnsureSuccessStatusOnly(response, "UNSUBSCRIBE");
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<StreamCommitEvent>? registration = null;
        bool shouldUnsubscribe = false;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
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
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();

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

    private void HandleNotification(byte[] payload)
    {
        try
        {
            var notifyReader = new BinaryBufferReader(payload);
            var subscriptionId = notifyReader.ReadU64();
            var route = notifyReader.ReadString();
            var bodyLength = notifyReader.ReadU32();
            var body = notifyReader.ReadSpan((int)bodyLength);

            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                var notification = new StreamCommitEvent(route, StreamWireHelpers.TryParseCommitOffset(body));
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
        ValidateStreamRoute(route, allowWildcardSelectors: false);
    }

    private static void ValidateStreamSelector(string route)
    {
        ValidateStreamRoute(route, allowWildcardSelectors: true);
    }

    private static void ValidateStreamRoute(string route, bool allowWildcardSelectors)
    {
        ArgumentNullException.ThrowIfNull(route);

        if (!route.StartsWith("stream://", StringComparison.Ordinal))
        {
            throw new StreamException($"stream route '{route}' must start with stream://", "INVALID_ROUTE");
        }

        var remainderStart = "stream://".Length;
        var firstSlash = route.IndexOf('/', remainderStart);
        var secondSlash = firstSlash >= 0 ? route.IndexOf('/', firstSlash + 1) : -1;
        var thirdSlash = secondSlash >= 0 ? route.IndexOf('/', secondSlash + 1) : -1;
        if (firstSlash < 0 || secondSlash < 0 || thirdSlash >= 0)
        {
            ThrowInvalidRouteShape(route, allowWildcardSelectors);
        }

        var realm = route.AsSpan(remainderStart, firstSlash - remainderStart);
        var area = route.AsSpan(firstSlash + 1, secondSlash - firstSlash - 1);
        var resource = route.AsSpan(secondSlash + 1);

        if (realm.IsEmpty || area.IsEmpty || resource.IsEmpty)
        {
            throw new StreamException($"stream route '{route}' segments must be non-empty", "INVALID_ROUTE");
        }

        if (!allowWildcardSelectors)
        {
            if (IsWildcardSegment(realm) || IsWildcardSegment(area) || IsWildcardSegment(resource))
            {
                throw new StreamException($"stream route '{route}' must be stream://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
            }

            return;
        }

        if (IsWildcardSegment(realm) || IsDoubleWildcard(area) || IsDoubleWildcard(resource))
        {
            ThrowInvalidSelectorRoute(route);
        }

        if (IsSingleWildcard(area) && !IsSingleWildcard(resource))
        {
            ThrowInvalidSelectorRoute(route);
        }
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
                entry.Subscription.SubscriptionId = subscriptionId;
                restoredSubscriptions[entry.Pattern] = entry.Subscription;
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

    private sealed class StreamSubscriptionState
    {
        public StreamSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; set; }

        public Dictionary<long, SubscriptionRegistration<StreamCommitEvent>> Registrations { get; } = new();

        public StreamSubscriptionState Clone()
        {
            var clone = new StreamSubscriptionState(SubscriptionId);
            foreach (var entry in Registrations)
            {
                clone.Registrations.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }

    private static void ThrowInvalidRouteShape(string route, bool allowWildcardSelectors)
    {
        var expected = allowWildcardSelectors
            ? "stream://{realm}/{area}/{resource}, stream://{realm}/{area}/*, or stream://{realm}/*/*"
            : "stream://{realm}/{area}/{resource}";
        throw new StreamException($"stream route '{route}' must be {expected}", "INVALID_ROUTE");
    }

    private static void ThrowInvalidSelectorRoute(string route)
    {
        throw new StreamException($"stream route '{route}' must be one of stream://{{realm}}/{{area}}/{{resource}}, stream://{{realm}}/{{area}}/*, or stream://{{realm}}/*/*", "INVALID_ROUTE");
    }

    private static bool IsSingleWildcard(ReadOnlySpan<char> segment)
    {
        return segment.Length == 1 && segment[0] == '*';
    }

    private static bool IsDoubleWildcard(ReadOnlySpan<char> segment)
    {
        return segment.Length == 2 && segment[0] == '*' && segment[1] == '*';
    }

    private static bool IsWildcardSegment(ReadOnlySpan<char> segment)
    {
        return IsSingleWildcard(segment) || IsDoubleWildcard(segment);
    }

    private static StreamRecord[] ParseReadRecords(ReadOnlyMemory<byte> data)
    {
        var reader = new BinaryBufferReader(data);
        if (reader.IsEof)
        {
            return Array.Empty<StreamRecord>();
        }

        if (reader.RemainingBytes < 4)
        {
            throw new StreamException("READ response missing record count", "READ_INVALID_RESPONSE");
        }

        var recordCount = reader.ReadU32();
        if (recordCount == 0)
        {
            if (!reader.IsEof)
            {
                throw new StreamException("READ response had trailing data", "READ_INVALID_RESPONSE");
            }

            return Array.Empty<StreamRecord>();
        }

        if (recordCount > int.MaxValue)
        {
            throw new StreamException("READ response record count too large", "READ_INVALID_RESPONSE");
        }

        var records = new StreamRecord[(int)recordCount];
        for (var index = 0; index < records.Length; index++)
        {
            if (reader.RemainingBytes < 12)
            {
                throw new StreamException("READ response truncated record", "READ_INVALID_RESPONSE");
            }

            var offset = reader.ReadU64();
            var bodyLength = checked((int)reader.ReadU32());
            if (reader.RemainingBytes < bodyLength)
            {
                throw new StreamException("READ response truncated record body", "READ_INVALID_RESPONSE");
            }

            records[index] = new StreamRecord(offset, reader.ReadBytes(bodyLength));
        }

        if (!reader.IsEof)
        {
            throw new StreamException("READ response had trailing data", "READ_INVALID_RESPONSE");
        }

        return records;
    }
}
