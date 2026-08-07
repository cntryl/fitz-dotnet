using System.Runtime.CompilerServices;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Threading.Channels;
using System.Collections.Concurrent;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseClient : ILeaseClient, IDisposable
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    private readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    private readonly Func<Action, IDisposable>? _registerOnDisconnect;
    private readonly Func<Func<CancellationToken, ValueTask>, bool>? _dispatchAsyncHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly SemaphoreSlim _acquisitionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, LeaseSubscriptionState> _subscriptionsByRoute = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _routesBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private IDisposable? _acquireNotificationRegistration;
    private readonly ConcurrentQueue<TaskCompletionSource<ReadOnlyMemory<byte>>> _queuedAcquisitions = new();
    private bool _acquireNotificationHandlerInitialized;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal LeaseClient(FitzConnection connection)
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

    public async Task<ILease> AcquireAsync(string route, ulong ttlSecs, uint waitSeconds = 0, CancellationToken ct = default)
    {
        await _acquisitionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await AcquireCoreAsync(route, ttlSecs, waitSeconds, ct).ConfigureAwait(false);
        }
        finally
        {
            _acquisitionGate.Release();
        }
    }

    private async Task<ILease> AcquireCoreAsync(string route, ulong ttlSecs, uint waitSeconds, CancellationToken ct)
    {
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new LeaseException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        TaskCompletionSource<ReadOnlyMemory<byte>>? deferred = null;
        if (waitSeconds > 0 && _registerNotificationHandler is not null)
        {
            EnsureAcquireNotificationHandlerInitialized();
            deferred = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
            _queuedAcquisitions.Enqueue(deferred);
        }
        using var cancellationRegistration = ct.Register(() => deferred?.TrySetCanceled(ct));
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU64(ttlSecs);
        writer.WriteU32(waitSeconds);
        ReadOnlyMemory<byte> response;
        try
        {
            response = await _request(MessageTypes.LeaseAcquire, writer.WrittenMemory, ct).ConfigureAwait(false);
        }
        catch
        {
            deferred?.TrySetCanceled(CancellationToken.None);
            RemoveQueuedAcquisition(deferred);
            throw;
        }
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            RemoveQueuedAcquisition(deferred);
            throw new LeaseException($"ACQUIRE failed with status {status}", "ACQUIRE_FAILED", status);
        }

        if (reader.RemainingBytes < 9)
        {
            RemoveQueuedAcquisition(deferred);
            throw new LeaseException("ACQUIRE response missing fencing token", "MISSING_TOKEN");
        }

        var responseType = reader.ReadU8();
        var fencedToken = reader.ReadU64();
        if (!reader.IsEof)
        {
            RemoveQueuedAcquisition(deferred);
            throw new LeaseException("ACQUIRE response has trailing bytes", "ACQUIRE_INVALID_RESPONSE");
        }

        if (responseType is 2 or 3)
        {
            if (deferred is null)
                throw new LeaseException("ACQUIRE queued without a wait request", "ACQUIRE_INVALID_RESPONSE");
            try
            {
                response = await deferred.Task.ConfigureAwait(false);
            }
            catch
            {
                RemoveQueuedAcquisition(deferred);
                throw;
            }
            reader = new BinaryBufferReader(response);
            status = reader.ReadU8();
            if (status != 0)
            {
                var message = reader.ReadString();
                throw new LeaseException($"ACQUIRE failed: {message}", "ACQUIRE_FAILED", status);
            }
            responseType = reader.ReadU8();
            fencedToken = reader.ReadU64();
            if (responseType is not 0 and not 1 || !reader.IsEof)
                throw new LeaseException("ACQUIRE deferred response is invalid", "ACQUIRE_INVALID_RESPONSE");
        }
        else
        {
            deferred?.TrySetCanceled(CancellationToken.None);
            RemoveQueuedAcquisition(deferred);
        }

        return new LeaseHandle(_request, route, fencedToken, _registerOnDisconnect);
    }

    private void RemoveQueuedAcquisition(TaskCompletionSource<ReadOnlyMemory<byte>>? target)
    {
        if (target is null) return;
        var retained = new List<TaskCompletionSource<ReadOnlyMemory<byte>>>();
        while (_queuedAcquisitions.TryDequeue(out var candidate))
        {
            if (!ReferenceEquals(candidate, target)) retained.Add(candidate);
        }
        foreach (var candidate in retained) _queuedAcquisitions.Enqueue(candidate);
    }

    private void EnsureAcquireNotificationHandlerInitialized()
    {
        if (_registerNotificationHandler is null)
            throw new InvalidOperationException("Notification handlers not configured for queued lease acquisition");
        lock (_gate)
        {
            if (_acquireNotificationHandlerInitialized) return;
            _acquireNotificationHandlerInitialized = true;
            _acquireNotificationRegistration = _registerNotificationHandler(MessageTypes.LeaseAcquire, payload =>
            {
                while (_queuedAcquisitions.TryDequeue(out var waiter))
                {
                    if (waiter.TrySetResult(payload)) break;
                }
            });
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Lease execution must aggregate arbitrary user callback, renewal, and cleanup failures.")]
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "The await-using declaration must retain the strongly typed lease handle for renewal operations.")]
    public async Task<T> WithLeaseAsync<T>(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ct.ThrowIfCancellationRequested();
        if (ttlSecs == 0 || ttlSecs > uint.MaxValue / 1000)
        {
            throw new LeaseException("ttlSecs must be positive and schedulable", "INVALID_TTL");
        }

        var waitSeconds = options?.WaitForAvailability == true ? options.WaitSeconds : 0;
        await using var lease = await AcquireAsync(route, ttlSecs, waitSeconds, ct).ConfigureAwait(false);

        using var lifecycle = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task<T> callbackTask;
        try
        {
            callbackTask = callback(lifecycle.Token).AsTask();
        }
        catch (Exception error)
        {
            callbackTask = Task.FromException<T>(error);
        }

        Exception? leaseLoss = null;
        while (!callbackTask.IsCompleted)
        {
            var delay = Task.Delay(TimeSpan.FromSeconds(ttlSecs / 3d), CancellationToken.None);
            var completed = await Task.WhenAny(callbackTask, delay).ConfigureAwait(false);
            if (completed == callbackTask)
            {
                break;
            }

            try
            {
                await lease.ExtendAsync(ttlSecs, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception error)
            {
                leaseLoss = new LeaseException("Lease ownership was lost", "LEASE_LOST", error);
                await lifecycle.CancelAsync().ConfigureAwait(false);
                break;
            }
        }

        T? value = default;
        Exception? callbackError = null;
        try
        {
            value = await callbackTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (lifecycle.IsCancellationRequested)
        {
            // The underlying lifecycle cause is returned below.
        }
        catch (Exception error)
        {
            callbackError = error;
        }

        Exception? releaseError = null;
        if (leaseLoss is null)
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await lease.ReleaseAsync(cleanup.Token).ConfigureAwait(false);
            }
            catch (Exception error)
            {
                releaseError = error;
            }
        }

        var failures = new List<Exception>(3);
        if (leaseLoss is not null) failures.Add(leaseLoss);
        if (callbackError is not null) failures.Add(callbackError);
        if (releaseError is not null) failures.Add(releaseError);
        if (ct.IsCancellationRequested) failures.Insert(0, new OperationCanceledException(ct));
        if (failures.Count > 1) throw new AggregateException(failures);
        if (failures.Count == 1) throw failures[0];
        return value!;
    }

    public async Task WithLeaseAsync(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(callback);
        await WithLeaseAsync(
            route,
            ttlSecs,
            async token =>
            {
                await callback(token).ConfigureAwait(false);
                return true;
            },
            options,
            ct).ConfigureAwait(false);
    }

    public async Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default)
    {
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new LeaseException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = _retryRequest is null
            ? await _request(MessageTypes.LeaseQuery, writer.WrittenMemory, ct).ConfigureAwait(false)
            : await _retryRequest(
                new RetryOperation("lease", "query", RetryClass.ReplayableRead),
                MessageTypes.LeaseQuery,
                writer.WrittenMemory,
                ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"QUERY failed with status {status}", "QUERY_FAILED", status);
        }

        var hasHolder = reader.ReadU8();
        if (hasHolder == 0)
        {
            if (reader.RemainingBytes != 4)
                throw new LeaseException("QUERY response missing pending_waiters", "QUERY_INVALID_RESPONSE");
            var pendingWaiters = reader.ReadU32();

            if (!reader.IsEof)
            {
                throw new LeaseException("QUERY response has trailing bytes", "QUERY_INVALID_RESPONSE");
            }

            return new LeaseInfo(false, PendingWaiters: pendingWaiters);
        }

        var owner = reader.ReadString();
        var ttlRemaining = reader.ReadU64();
        if (reader.RemainingBytes != 4)
            throw new LeaseException("QUERY response missing pending_waiters", "QUERY_INVALID_RESPONSE");
        var heldPendingWaiters = reader.ReadU32();

        if (!reader.IsEof)
        {
            throw new LeaseException("QUERY response has trailing bytes", "QUERY_INVALID_RESPONSE");
        }

        return new LeaseInfo(true, owner, ttlRemaining, heldPendingWaiters);
    }

    public async Task<LeaseSubscription> SubscribeAsync(
        string route,
        CancellationToken ct = default)
    {
        var buffer = new AsyncSubscriptionBuffer<LeaseChangeEvent>(route);
        var registration = await SubscribeAsync(route, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, ct).ConfigureAwait(false);
        return new LeaseSubscription(route, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            buffer.Complete();
            await registration.UnsubscribeAsync(token).ConfigureAwait(false);
        });
    }

    internal async Task<LeaseSubscription> SubscribeAsync(
        string route,
        Func<LeaseChangeEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new LeaseException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
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
        SubscriptionRegistration<LeaseChangeEvent>? registration = null;
        var handleId = Interlocked.Increment(ref _nextHandleId);
        var gateAcquired = false;

        try
        {
            registration = new SubscriptionRegistration<LeaseChangeEvent>(channel);
            await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
            gateAcquired = true;

            if (_subscriptionsByRoute.TryGetValue(route, out var existingSubscription))
            {
                existingSubscription.Registrations[handleId] = registration;
                var existingHandle = CreateSubscription(route, handleId);
                SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                registration = null;
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(route, ct).ConfigureAwait(false);
            var subscription = new LeaseSubscriptionState(subscriptionId);
            subscription.Registrations[handleId] = registration;
            _subscriptionsByRoute[route] = subscription;
            _routesBySubscriptionId[subscriptionId] = route;

            var handle = CreateSubscription(route, handleId);
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

    private LeaseSubscription CreateSubscription(string route, long handleId)
    {
        return new LeaseSubscription(
            route,
            cancellationToken => UnsubscribeAsync(route, handleId, cancellationToken));
    }

    private async Task<ulong> SubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.LeaseSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            var message = reader.ReadString();
            throw new LeaseException($"SUBSCRIBE failed: {message}", "SUBSCRIBE_FAILED", status);
        }

        if (reader.RemainingBytes != 8)
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

    private async Task UnsubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.LeaseUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            var message = reader.ReadString();
            throw new LeaseException($"UNSUBSCRIBE failed: {message}", "UNSUBSCRIBE_FAILED", status);
        }
    }

    private async ValueTask UnsubscribeAsync(string route, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<LeaseChangeEvent>? registration = null;
        bool shouldUnsubscribe = false;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_subscriptionsByRoute.TryGetValue(route, out var subscription))
            {
                return;
            }

            if (!subscription.Registrations.Remove(handleId, out registration))
            {
                return;
            }

            if (subscription.Registrations.Count == 0)
            {
                _subscriptionsByRoute.Remove(route);
                _routesBySubscriptionId.Remove(subscription.SubscriptionId);
                shouldUnsubscribe = true;
            }
        }
        finally
        {
            _subscriptionGate.Release();
            registration?.Dispose();
        }

        if (shouldUnsubscribe)
        {
            await UnsubscribeWireAsync(route, ct).ConfigureAwait(false);
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

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed broker notifications are dropped without disrupting the receive loop.")]
    private void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var route = reader.ReadString();
            if (reader.ReadU32() != 0 || !reader.IsEof)
            {
                return;
            }
            lock (_gate)
            {
                if (!_routesBySubscriptionId.TryGetValue(subscriptionId, out var registeredRoute) ||
                    !_subscriptionsByRoute.TryGetValue(registeredRoute, out var subscription))
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
            List<(string Route, LeaseSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByRoute.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Route, LeaseSubscriptionState Subscription)>(_subscriptionsByRoute.Count);
                foreach (var entry in _subscriptionsByRoute)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, LeaseSubscriptionState>(StringComparer.Ordinal);
            var restoredRoutesById = new Dictionary<ulong, string>();

            foreach (var entry in snapshot)
            {
                var subscriptionId = await SubscribeWireAsync(entry.Route, cancellationToken).ConfigureAwait(false);
                restoredSubscriptions[entry.Route] = entry.Subscription.Clone(subscriptionId);
                restoredRoutesById[subscriptionId] = entry.Route;
            }

            lock (_gate)
            {
                _subscriptionsByRoute.Clear();
                _routesBySubscriptionId.Clear();

                foreach (var entry in restoredSubscriptions)
                {
                    _subscriptionsByRoute[entry.Key] = entry.Value;
                }

                foreach (var entry in restoredRoutesById)
                {
                    _routesBySubscriptionId[entry.Key] = entry.Value;
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
        _acquireNotificationRegistration?.Dispose();
        while (_queuedAcquisitions.TryDequeue(out var waiter)) waiter.TrySetCanceled();
        _reconnectRegistration?.Dispose();
        lock (_gate)
        {
            foreach (var subscription in _subscriptionsByRoute.Values)
            {
                foreach (var registration in subscription.Registrations.Values)
                {
                    registration.Dispose();
                }
            }

            _subscriptionsByRoute.Clear();
            _routesBySubscriptionId.Clear();
        }

        _subscriptionGate.Dispose();
        _acquisitionGate.Dispose();
    }

    private sealed class LeaseSubscriptionState
    {
        public LeaseSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; init; }

        public Dictionary<long, SubscriptionRegistration<LeaseChangeEvent>> Registrations { get; } = new();

        public LeaseSubscriptionState Clone() => Clone(SubscriptionId);

        public LeaseSubscriptionState Clone(ulong subscriptionId)
        {
            var clone = new LeaseSubscriptionState(subscriptionId);
            foreach (var entry in Registrations)
            {
                clone.Registrations.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }
}
