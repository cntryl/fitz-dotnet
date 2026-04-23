using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseClient : ILeaseClient
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, LeaseSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal LeaseClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterBorrowedNotificationHandler)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public LeaseClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
    }

    internal LeaseClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
    }

    public async ValueTask<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken ct = default)
    {
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new LeaseException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU64(ttlSecs);
        var response = await _request(MessageTypes.LeaseAcquire, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"ACQUIRE failed with status {status}", "ACQUIRE_FAILED", status);
        }

        if (reader.RemainingBytes == 8)
        {
            var token = reader.ReadU64();
            if (!reader.IsEof)
            {
                throw new LeaseException("ACQUIRE response has trailing bytes", "ACQUIRE_INVALID_RESPONSE");
            }

            return new LeaseHandle(_request, route, token);
        }

        if (reader.RemainingBytes < 9)
        {
            throw new LeaseException("ACQUIRE response missing fencing token", "MISSING_TOKEN");
        }

        var responseType = reader.ReadU8();
        if (responseType >= 2)
        {
            throw new LeaseException($"ACQUIRE returned non-acquired response type {responseType}", "ACQUIRE_NOT_ACQUIRED");
        }

        var fencedToken = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new LeaseException("ACQUIRE response has trailing bytes", "ACQUIRE_INVALID_RESPONSE");
        }

        return new LeaseHandle(_request, route, fencedToken);
    }

    public async ValueTask<LeaseInfo> QueryAsync(string route, CancellationToken ct = default)
    {
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new LeaseException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.LeaseQuery, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"QUERY failed with status {status}", "QUERY_FAILED", status);
        }

        var hasHolder = reader.ReadU8();
        if (hasHolder == 0)
        {
            if (!reader.IsEof)
            {
                if (reader.RemainingBytes < 4)
                {
                    throw new LeaseException("QUERY response has trailing bytes", "QUERY_INVALID_RESPONSE");
                }

                _ = reader.ReadU32();
            }

            if (!reader.IsEof)
            {
                throw new LeaseException("QUERY response has trailing bytes", "QUERY_INVALID_RESPONSE");
            }

            return new LeaseInfo(false);
        }

        var owner = reader.ReadString();
        var ttlRemaining = reader.ReadU64();
        if (!reader.IsEof)
        {
            if (reader.RemainingBytes < 4)
            {
                throw new LeaseException("QUERY response has trailing bytes", "QUERY_INVALID_RESPONSE");
            }

            _ = reader.ReadU32();
        }

        if (!reader.IsEof)
        {
            throw new LeaseException("QUERY response has trailing bytes", "QUERY_INVALID_RESPONSE");
        }

        return new LeaseInfo(true, owner, ttlRemaining);
    }

    public async Task<LeaseSubscription> SubscribeAsync(
        string pattern,
        Func<LeaseChangeEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsFixedRoute(pattern, "lease", 3))
        {
            throw new LeaseException($"route '{pattern}' must be lease://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<LeaseChangeEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        var registration = new SubscriptionRegistration<LeaseChangeEvent>(channel);
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
            var subscription = new LeaseSubscriptionState(subscriptionId);
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

    private LeaseSubscription CreateSubscription(string pattern, long handleId, ulong subscriptionId)
    {
        return new LeaseSubscription(
            subscriptionId,
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken));
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.LeaseSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
        }

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new LeaseException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        var subscriptionId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new LeaseException("SUBSCRIBE response has trailing bytes", "SUBSCRIBE_INVALID_RESPONSE");
        }

        return subscriptionId;
    }

    private async Task UnsubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.LeaseUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"UNSUBSCRIBE failed with status {status}", "UNSUBSCRIBE_FAILED", status);
        }
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<LeaseChangeEvent>? registration = null;
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
        _notificationRegistration = _registerNotificationHandler(MessageTypes.LeaseNotify, HandleNotification);
    }

    private void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var route = reader.ReadString();
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                var notification = new LeaseChangeEvent(route);
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
            List<(string Pattern, LeaseSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByPattern.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Pattern, LeaseSubscriptionState Subscription)>(_subscriptionsByPattern.Count);
                foreach (var entry in _subscriptionsByPattern)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, LeaseSubscriptionState>(StringComparer.Ordinal);
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

    private sealed class LeaseSubscriptionState
    {
        public LeaseSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; set; }

        public Dictionary<long, SubscriptionRegistration<LeaseChangeEvent>> Registrations { get; } = new();

        public LeaseSubscriptionState Clone()
        {
            var clone = new LeaseSubscriptionState(SubscriptionId);
            foreach (var entry in Registrations)
            {
                clone.Registrations.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }
}
