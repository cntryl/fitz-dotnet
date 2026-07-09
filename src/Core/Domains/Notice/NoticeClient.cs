using System.Collections.Generic;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Notice;

public sealed class NoticeClient : INoticeClient
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _request;
    private readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    private readonly Func<Func<CancellationToken, ValueTask>, bool>? _dispatchAsyncHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, NoticeSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal NoticeClient(FitzConnection connection)
        : this(
            connection.SendAsync,
            connection.RequestAsync,
            connection.RegisterBorrowedNotificationHandler,
            connection.TryDispatchAsyncHandler)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public NoticeClient(
        Func<ushort, byte[], CancellationToken, Task> send,
        Func<ushort, byte[], CancellationToken, Task<byte[]>>? request = null,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => await send(messageType, payload.ToArray(), ct).ConfigureAwait(false),
            request is null
                ? null
                : async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
    }

    internal NoticeClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? request = null,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null)
    {
        _send = send;
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _dispatchAsyncHandler = dispatchAsyncHandler;
    }

    public ValueTask PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default)
    {
        if (!RouteValidation.IsFixedRoute(route, "notice", 3))
        {
            throw new NoticeException($"route '{route}' must be notice://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);
        return _send(MessageTypes.NoticePublish, writer.WrittenMemory, ct);
    }

    public async Task<NoticeSubscription> SubscribeAsync(string pattern, Func<NoticeMessage, CancellationToken, ValueTask> handler, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsSelectorRoute(pattern, "notice", 3, allowRealmWildcard: true))
        {
            throw new NoticeException($"route '{pattern}' must be notice://{{realm}}/{{area}}/{{resource}}, notice://{{realm}}/{{area}}/*, or notice://{{realm}}/*/*", "INVALID_ROUTE");
        }

        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<NoticeMessage>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        var registration = new SubscriptionRegistration<NoticeMessage>(channel);

        var handleId = Interlocked.Increment(ref _nextHandleId);
        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_subscriptionsByPattern.TryGetValue(pattern, out var existingSubscription))
            {
                existingSubscription.Writers[handleId] = registration;
                var existingHandle = CreateSubscription(pattern, handleId);
                SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new NoticeSubscriptionState(subscriptionId);
            subscription.Writers[handleId] = registration;
            _subscriptionsByPattern[pattern] = subscription;
            _patternsBySubscriptionId[subscriptionId] = pattern;

            var handle = CreateSubscription(pattern, handleId);
            SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
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

    private NoticeSubscription CreateSubscription(
        string pattern,
        long handleId)
    {
        return new NoticeSubscription(
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken));
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        ulong? subscriptionId = null;
        SubscriptionRegistration<NoticeMessage>? registration = null;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
            {
                return;
            }

            if (!subscription.Writers.Remove(handleId, out registration))
            {
                return;
            }

            if (subscription.Writers.Count == 0)
            {
                subscriptionId = subscription.SubscriptionId;
                _subscriptionsByPattern.Remove(pattern);
                _patternsBySubscriptionId.Remove(subscription.SubscriptionId);
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();

        if (subscriptionId.HasValue)
        {
            await UnsubscribeWireAsync(subscriptionId.Value, ct).ConfigureAwait(false);
        }
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice subscriptions");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.NoticeSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new NoticeException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
        }

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new NoticeException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        var subscriptionId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new NoticeException("SUBSCRIBE response has trailing bytes", "SUBSCRIBE_INVALID_RESPONSE");
        }

        return subscriptionId;
    }

    private async Task UnsubscribeWireAsync(ulong subscriptionId, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice unsubscription");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(subscriptionId);

        var response = await _request(MessageTypes.NoticeUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new NoticeException($"UNSUBSCRIBE failed with status {status}", "UNSUBSCRIBE_FAILED", status);
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
        _notificationRegistration = _registerNotificationHandler(MessageTypes.NoticeNotify, HandleNotification);
    }

    private void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var route = reader.ReadString();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes((int)bodyLength);
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                var message = new NoticeMessage(route, body);
                foreach (var registration in subscription.Writers.Values)
                {
                    registration.Channel.Writer.TryWrite(message);
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

    private async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<(string Pattern, NoticeSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByPattern.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Pattern, NoticeSubscriptionState Subscription)>(_subscriptionsByPattern.Count);
                foreach (var entry in _subscriptionsByPattern)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, NoticeSubscriptionState>(StringComparer.Ordinal);
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

    private sealed class NoticeSubscriptionState
    {
        public NoticeSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; init; }

        public Dictionary<long, SubscriptionRegistration<NoticeMessage>> Writers { get; } = new();

        public NoticeSubscriptionState Clone() => Clone(SubscriptionId);

        public NoticeSubscriptionState Clone(ulong subscriptionId)
        {
            var clone = new NoticeSubscriptionState(subscriptionId);
            foreach (var entry in Writers)
            {
                clone.Writers.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }
}
