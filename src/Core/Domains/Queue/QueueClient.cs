using System.Runtime.CompilerServices;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Queue;

public sealed class QueueClient : IQueueClient, IDisposable
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    private readonly Func<Func<CancellationToken, ValueTask>, bool>? _dispatchAsyncHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, QueueSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private readonly Func<Action, IDisposable>? _registerOnDisconnect;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal QueueClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterBorrowedNotificationHandler,
            connection.OnDisconnect,
            connection.TryDispatchAsyncHandler)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public QueueClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
    }

    internal QueueClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _registerOnDisconnect = registerOnDisconnect;
        _dispatchAsyncHandler = dispatchAsyncHandler;
    }

    public async ValueTask<ulong> EnqueueAsync(
        string route,
        ReadOnlyMemory<byte> body,
        int? delayMs = null,
        CancellationToken ct = default)
    {
        if (!RouteValidation.IsFixedRoute(route, "queue", 3))
        {
            throw new QueueException($"route '{route}' must be queue://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);

        var delaySeconds = (delayMs ?? 0) / 1000;
        writer.WriteU8((byte)(delaySeconds > 0 ? 1 : 0));
        if (delaySeconds > 0)
        {
            writer.WriteU64((ulong)delaySeconds);
        }

        var response = await _request(MessageTypes.QueueEnqueue, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"ENQUEUE failed with status {status}", "ENQUEUE_FAILED", status);
        }

        var result = reader.IsEof ? 0UL : reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new QueueException("ENQUEUE response has trailing bytes", "ENQUEUE_INVALID_RESPONSE");
        }

        return result;
    }

    public async Task<IQueueReservedItem[]> ReserveAsync(
        string route,
        ulong leaseSeconds,
        int batchSize = 1,
        int? waitSeconds = null,
        CancellationToken ct = default)
    {
        if (!RouteValidation.IsSelectorRoute(route, "queue", 3, allowRealmWildcard: false))
        {
            throw new QueueException($"route '{route}' must be queue://{{realm}}/{{area}}/{{resource}} or queue://{{realm}}/{{area}}/*", "INVALID_ROUTE");
        }

        var normalizedBatchSize = batchSize > 0 ? batchSize : 1;

        if (!waitSeconds.HasValue || waitSeconds.Value <= 0)
        {
            return await ReserveOnceAsync(route, leaseSeconds, normalizedBatchSize, ct).ConfigureAwait(false);
        }

        var deadline = DateTime.UtcNow.AddSeconds(waitSeconds.Value);
        while (true)
        {
            var items = await ReserveOnceAsync(route, leaseSeconds, normalizedBatchSize, ct).ConfigureAwait(false);
            if (items.Length > 0)
            {
                return items;
            }

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                return items;
            }

            var delay = remaining <= TimeSpan.FromMilliseconds(100)
                ? remaining
                : TimeSpan.FromMilliseconds(100);
            await Task.Delay(delay, ct).ConfigureAwait(false);
        }
    }

    private async Task<IQueueReservedItem[]> ReserveOnceAsync(
        string route,
        ulong leaseSeconds,
        int batchSize,
        CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(leaseSeconds);
        writer.WriteU8((byte)(batchSize > 0 ? 1 : 0));
        if (batchSize > 0)
        {
            writer.WriteU32((uint)batchSize);
        }

        var response = await _request(MessageTypes.QueueReserve, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"RESERVE failed with status {status}", "RESERVE_FAILED", status);
        }

        var count = reader.IsEof ? 0U : reader.ReadU32();
        var items = new IQueueReservedItem[count];
        for (var i = 0; i < count; i++)
        {
            var itemId = reader.ReadU64();
            var itemToken = reader.ReadU64();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes((int)bodyLength);

            items[i] = new QueueReservedItem(
                route,
                body,
                1,
                itemId,
                itemToken,
                _request,
                _registerOnDisconnect);
        }

        if (!reader.IsEof)
        {
            throw new QueueException("RESERVE response has trailing bytes", "RESERVE_INVALID_RESPONSE");
        }

        return items;
    }

    public async Task<QueueSubscription> SubscribeAsync(
        string pattern,
        Func<QueueAvailabilityEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsSelectorRoute(pattern, "queue", 3, allowRealmWildcard: true))
        {
            throw new QueueException($"route '{pattern}' must be queue://{{realm}}/{{area}}/{{resource}}, queue://{{realm}}/{{area}}/*, or queue://{{realm}}/*/*", "INVALID_ROUTE");
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<QueueAvailabilityEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        SubscriptionRegistration<QueueAvailabilityEvent>? registration = null;
        var handleId = Interlocked.Increment(ref _nextHandleId);
        var gateAcquired = false;

        try
        {
            registration = new SubscriptionRegistration<QueueAvailabilityEvent>(channel);
            await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
            gateAcquired = true;

            if (_subscriptionsByPattern.TryGetValue(pattern, out var existingSubscription))
            {
                existingSubscription.Registrations[handleId] = registration;
                var existingHandle = CreateSubscription(pattern, handleId);
                SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                registration = null;
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new QueueSubscriptionState(subscriptionId);
            subscription.Registrations[handleId] = registration;
            _subscriptionsByPattern[pattern] = subscription;
            _patternsBySubscriptionId[subscriptionId] = pattern;

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

    private QueueSubscription CreateSubscription(string pattern, long handleId)
    {
        return new QueueSubscription(
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken));
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.QueueSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
        }

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new QueueException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        var subscriptionId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new QueueException("SUBSCRIBE response has trailing bytes", "SUBSCRIBE_INVALID_RESPONSE");
        }

        return subscriptionId;
    }

    private async Task UnsubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.QueueUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"UNSUBSCRIBE failed with status {status}", "UNSUBSCRIBE_FAILED", status);
        }
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<QueueAvailabilityEvent>? registration = null;
        ulong? subscriptionId = null;

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
                subscriptionId = subscription.SubscriptionId;
                _subscriptionsByPattern.Remove(pattern);
                _patternsBySubscriptionId.Remove(subscription.SubscriptionId);
            }
        }
        finally
        {
            _subscriptionGate.Release();
            registration?.Dispose();
        }

        if (subscriptionId.HasValue)
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
        _notificationRegistration = _registerNotificationHandler(MessageTypes.QueueNotify, HandleNotification);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed broker notifications are dropped without disrupting the receive loop.")]
    private void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var eventRoute = reader.ReadString();
            var messageCount = reader.ReadU64();
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                var notification = new QueueAvailabilityEvent(eventRoute, messageCount);
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

    private async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<(string Pattern, QueueSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByPattern.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Pattern, QueueSubscriptionState Subscription)>(_subscriptionsByPattern.Count);
                foreach (var entry in _subscriptionsByPattern)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, QueueSubscriptionState>(StringComparer.Ordinal);
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

    private sealed class QueueSubscriptionState
    {
        public QueueSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; init; }

        public Dictionary<long, SubscriptionRegistration<QueueAvailabilityEvent>> Registrations { get; } = new();

        public QueueSubscriptionState Clone() => Clone(SubscriptionId);

        public QueueSubscriptionState Clone(ulong subscriptionId)
        {
            var clone = new QueueSubscriptionState(subscriptionId);
            foreach (var entry in Registrations)
            {
                clone.Registrations.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }
}
