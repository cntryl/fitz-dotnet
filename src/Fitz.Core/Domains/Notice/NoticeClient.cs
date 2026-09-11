using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Notice;

public sealed class NoticeClient : INoticeClient, IDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _request;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly int _subscriptionBufferCapacity;
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Client disposal may race active subscriptions; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    readonly object _gate = new();
    readonly Dictionary<string, NoticeSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    readonly Dictionary<ulong, string> _patternsBySubscriptionId = [];
    IDisposable? _notificationRegistration;
    bool _notificationHandlerInitialized;
    int _disposed;
    long _nextHandleId;
    readonly IDisposable? _reconnectRegistration;

    internal NoticeClient(FitzConnection connection)
        : this(
            connection.SendAsync,
            connection.RequestAsync,
            connection.RegisterBorrowedNotificationHandler,
            (handler, rejected) => connection.TryDispatchAsyncHandler("notice", handler, rejected),
            connection.SubscriptionBufferCapacity)
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
        ArgumentNullException.ThrowIfNull(send);
    }

    internal NoticeClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? request = null,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        int subscriptionBufferCapacity = SubscriptionRegistration<NoticeMessage>.DefaultCapacity)
    {
        _send = send;
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _subscriptionBufferCapacity = subscriptionBufferCapacity;
    }

    public async Task PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "notice", 3))
        {
            throw new NoticeException($"route '{route}' must be notice://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);
        await _send(MessageTypes.NoticePublish, writer.WrittenMemory, ct).ConfigureAwait(false);
    }

    public async Task<NoticeSubscription> SubscribeAsync(string pattern, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var buffer = new AsyncSubscriptionBuffer<NoticeMessage>(pattern, _subscriptionBufferCapacity);
        var registration = await SubscribeAsync(pattern, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, ct).ConfigureAwait(false);
        buffer.ObserveCompletion(registration.Completion);
        return new NoticeSubscription(pattern, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            await registration.UnsubscribeAsync(token).ConfigureAwait(false);
            buffer.Complete();
        }, registration.Completion);
    }

    internal async Task<NoticeSubscription> SubscribeAsync(string pattern, Func<NoticeMessage, CancellationToken, ValueTask> handler, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsRegistrationPattern(pattern, "notice"))
        {
            throw new NoticeException($"pattern '{pattern}' must use whole-segment * or ** wildcards", "INVALID_ROUTE");
        }

        EnsureNotificationHandlerInitialized();

        var channel = SubscriptionRegistration<NoticeMessage>.CreateChannel(_subscriptionBufferCapacity);
        SubscriptionRegistration<NoticeMessage>? registration = null;

        var handleId = Interlocked.Increment(ref _nextHandleId);
        var gateAcquired = false;
        try
        {
            registration = new SubscriptionRegistration<NoticeMessage>(
                channel,
                "notice",
                pattern,
                token => UnsubscribeAsync(pattern, handleId, token));
            await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
            gateAcquired = true;

            lock (_gate)
            {
                if (_subscriptionsByPattern.TryGetValue(pattern, out var existingSubscription))
                {
                    existingSubscription.Writers[handleId] = registration;
                    var existingHandle = CreateSubscription(pattern, handleId, registration.Completion);
                    SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                    registration = null;
                    return existingHandle;
                }
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new NoticeSubscriptionState(subscriptionId);
            subscription.Writers[handleId] = registration;
            lock (_gate)
            {
                _subscriptionsByPattern[pattern] = subscription;
                _patternsBySubscriptionId[subscriptionId] = pattern;
            }

            var handle = CreateSubscription(pattern, handleId, registration.Completion);
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

    NoticeSubscription CreateSubscription(
        string pattern,
        long handleId,
        Task completion)
    {
        return new NoticeSubscription(
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken),
            completion);
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership intentionally remains in the subscription table when the wire unsubscribe fails.")]
    async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<NoticeMessage>? registration = null;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ulong subscriptionId;
            var shouldUnsubscribe = false;
            lock (_gate)
            {
                if (!_subscriptionsByPattern.TryGetValue(pattern, out var subscription) ||
                    !subscription.Writers.TryGetValue(handleId, out registration))
                {
                    return;
                }

                subscriptionId = subscription.SubscriptionId;
                shouldUnsubscribe = subscription.Writers.Count == 1;
            }

            if (shouldUnsubscribe)
            {
                await UnsubscribeWireAsync(subscriptionId, ct).ConfigureAwait(false);
            }

            lock (_gate)
            {
                if (!_subscriptionsByPattern.TryGetValue(pattern, out var subscription) ||
                    !subscription.Writers.Remove(handleId, out registration))
                {
                    return;
                }

                if (subscription.Writers.Count == 0)
                {
                    _subscriptionsByPattern.Remove(pattern);
                    _patternsBySubscriptionId.Remove(subscription.SubscriptionId);
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();

    }

    async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice subscriptions");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.NoticeSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = NoticeWireHelpers.ReadSuccess(response, "SUBSCRIBE");

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

    async Task UnsubscribeWireAsync(ulong subscriptionId, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice unsubscription");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(subscriptionId);

        var response = await _request(MessageTypes.NoticeUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = NoticeWireHelpers.ReadSuccess(response, "UNSUBSCRIBE");
        if (!reader.IsEof)
        {
            throw new NoticeException("UNSUBSCRIBE response has trailing bytes", "UNSUBSCRIBE_INVALID_RESPONSE");
        }
    }

    void EnsureNotificationHandlerInitialized()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_notificationHandlerInitialized)
            {
                return;
            }

            if (_registerNotificationHandler is null)
            {
                throw new InvalidOperationException("Notification handlers not configured for subscription support");
            }

            _notificationRegistration = _registerNotificationHandler(MessageTypes.NoticeNotify, HandleNotification);
            _notificationHandlerInitialized = true;
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed broker notifications are dropped without disrupting the receive loop.")]
    void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var route = reader.ReadString();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes(bodyLength);
            if (!reader.IsEof)
            {
                return;
            }
            SubscriptionRegistration<NoticeMessage>[] registrations;
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                registrations = [.. subscription.Writers.Values];
            }

            var message = new NoticeMessage(route, body);
            foreach (var registration in registrations)
            {
                if (!registration.Channel.Writer.TryWrite(message))
                    _ = registration.FailOverflowAsync();
            }
        }
        catch
        {
        }
    }

    async ValueTask HandleReconnect(CancellationToken cancellationToken) => await RestoreSubscriptionsAsync(cancellationToken).ConfigureAwait(false);

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect restoration must best-effort roll back every already-restored subscription before preserving the original failure.")]
    async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
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

            try
            {
                foreach (var entry in snapshot)
                {
                    var subscriptionId = await SubscribeWireAsync(entry.Pattern, cancellationToken).ConfigureAwait(false);
                    restoredSubscriptions[entry.Pattern] = entry.Subscription.Clone(subscriptionId);
                    restoredPatternsById[subscriptionId] = entry.Pattern;
                }
            }
            catch
            {
                foreach (var subscriptionId in restoredPatternsById.Keys)
                {
                    try
                    {
                        await UnsubscribeWireAsync(subscriptionId, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Best effort; preserve the original restore failure.
                    }
                }

                throw;
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

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _notificationRegistration?.Dispose();
        _reconnectRegistration?.Dispose();
        lock (_gate)
        {
            foreach (var subscription in _subscriptionsByPattern.Values)
            {
                foreach (var registration in subscription.Writers.Values)
                {
                    registration.Dispose();
                }
            }

            _subscriptionsByPattern.Clear();
            _patternsBySubscriptionId.Clear();
        }

    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    sealed class NoticeSubscriptionState
    {
        public NoticeSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; init; }

        public Dictionary<long, SubscriptionRegistration<NoticeMessage>> Writers { get; } = [];

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
