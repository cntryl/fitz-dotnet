using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseClient : ILeaseClient, IDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    readonly Func<Action, IDisposable>? _registerOnDisconnect;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly Action<Exception>? _invalidateSession;
    readonly int _subscriptionBufferCapacity;
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Client disposal may race active subscriptions; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Client disposal may race an active acquisition; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _acquisitionGate = new(1, 1);
    readonly object _gate = new();
    readonly Dictionary<string, LeaseSubscriptionState> _subscriptionsByRoute = new(StringComparer.Ordinal);
    readonly Dictionary<ulong, string> _routesBySubscriptionId = [];
    IDisposable? _notificationRegistration;
    int _disposed;
    IDisposable? _acquireNotificationRegistration;
    readonly IDisposable? _acquisitionDisconnectRegistration;
    readonly ConcurrentQueue<TaskCompletionSource<ReadOnlyMemory<byte>>> _queuedAcquisitions = new();
    bool _acquireNotificationHandlerInitialized;
    bool _notificationHandlerInitialized;
    int _acquisitionLanePoisoned;
    long _nextHandleId;
    readonly IDisposable? _reconnectRegistration;
    readonly List<Func<CancellationToken, ValueTask>> _reconnectListeners = [];
    readonly object _reconnectListenersGate = new();

    internal LeaseClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterBorrowedNotificationHandler,
            connection.OnDisconnect,
            (handler, rejected) => connection.TryDispatchAsyncHandler("lease", handler, rejected),
            (operation, messageType, payload, cancellationToken) =>
                connection.ExecuteWithRetryAsync(
                    operation,
                    innerToken => connection.RequestAsync(messageType, payload, innerToken),
                    cancellationToken),
            connection.SubscriptionBufferCapacity,
            connection.InvalidateSession)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
        _acquisitionDisconnectRegistration = connection.OnDisconnect(HandleAcquisitionDisconnect);
    }

    public LeaseClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
        ArgumentNullException.ThrowIfNull(request);
    }

    internal LeaseClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retryRequest = null,
        int subscriptionBufferCapacity = SubscriptionRegistration<LeaseChangeEvent>.DefaultCapacity,
        Action<Exception>? invalidateSession = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _registerOnDisconnect = registerOnDisconnect;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _retryRequest = retryRequest;
        _subscriptionBufferCapacity = subscriptionBufferCapacity;
        _invalidateSession = invalidateSession;
    }

    public async Task<ILease> AcquireAsync(string route, ulong ttlSecs, uint waitSeconds = 0, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return await AcquireLeaseAsync(route, ttlSecs, waitSeconds, ct).ConfigureAwait(false);
    }

    async Task<LeaseHandle> AcquireLeaseAsync(string route, ulong ttlSecs, uint waitSeconds, CancellationToken ct)
    {
        if (ttlSecs == 0 || ttlSecs > uint.MaxValue / 1000)
        {
            throw new LeaseException("ttlSecs must be positive and schedulable", "INVALID_TTL");
        }

        await _acquisitionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _acquisitionLanePoisoned) != 0)
            {
                throw new ConnectionException("Queued lease acquisition requires a new connection session after an ambiguous result.");
            }

            return await AcquireCoreAsync(route, ttlSecs, waitSeconds, ct).ConfigureAwait(false);
        }
        finally
        {
            _acquisitionGate.Release();
        }
    }

    async Task<LeaseHandle> AcquireCoreAsync(string route, ulong ttlSecs, uint waitSeconds, CancellationToken ct)
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
        BinaryBufferReader reader;
        try
        {
            reader = LeaseWireHelpers.ReadSuccess(response, "ACQUIRE");
        }
        catch
        {
            RemoveQueuedAcquisition(deferred);
            throw;
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
            if (responseType is 2 or 3)
            {
                PoisonAcquisitionLane(new LeaseException(
                    "ACQUIRE queued response was malformed and left the positional grant lane ambiguous.",
                    "ACQUIRE_INVALID_RESPONSE"));
            }
            RemoveQueuedAcquisition(deferred);
            throw new LeaseException("ACQUIRE response has trailing bytes", "ACQUIRE_INVALID_RESPONSE");
        }

        if (responseType is 2 or 3)
        {
            if (deferred is null)
                throw new LeaseException("ACQUIRE queued without a wait request", "ACQUIRE_INVALID_RESPONSE");
            try
            {
                response = await deferred.Task.WaitAsync(TimeSpan.FromSeconds(waitSeconds), ct).ConfigureAwait(false);
            }
            catch (TimeoutException exception)
            {
                PoisonAcquisitionLane(exception);
                RemoveQueuedAcquisition(deferred);
                throw new RequestTimeoutException($"Queued lease acquisition exceeded its {waitSeconds}-second wait ceiling", exception);
            }
            catch (Exception exception)
            {
                PoisonAcquisitionLane(exception);
                RemoveQueuedAcquisition(deferred);
                throw;
            }
            try
            {
                reader = LeaseWireHelpers.ReadSuccess(response, "ACQUIRE");
                if (reader.RemainingBytes < 9)
                    throw new LeaseException("ACQUIRE deferred response missing fencing token", "ACQUIRE_INVALID_RESPONSE");
                responseType = reader.ReadU8();
                fencedToken = reader.ReadU64();
                if (responseType is not 0 and not 1 || !reader.IsEof)
                    throw new LeaseException("ACQUIRE deferred response is invalid", "ACQUIRE_INVALID_RESPONSE");
            }
            catch (Exception exception)
            {
                PoisonAcquisitionLane(exception);
                throw;
            }
        }
        else
        {
            deferred?.TrySetCanceled(CancellationToken.None);
            RemoveQueuedAcquisition(deferred);
        }

        return new LeaseHandle(_request, route, fencedToken, _registerOnDisconnect);
    }

    void RemoveQueuedAcquisition(TaskCompletionSource<ReadOnlyMemory<byte>>? target)
    {
        if (target is null)
            return;
        var retained = new List<TaskCompletionSource<ReadOnlyMemory<byte>>>();
        while (_queuedAcquisitions.TryDequeue(out var candidate))
        {
            if (!ReferenceEquals(candidate, target))
                retained.Add(candidate);
        }
        foreach (var candidate in retained)
            _queuedAcquisitions.Enqueue(candidate);
    }

    void PoisonAcquisitionLane(Exception exception)
    {
        Volatile.Write(ref _acquisitionLanePoisoned, 1);
        _invalidateSession?.Invoke(new ConnectionException(
            "The queued lease acquisition lane became ambiguous and requires a new connection session.",
            exception));
    }

    void HandleAcquisitionDisconnect()
    {
        Volatile.Write(ref _acquisitionLanePoisoned, 1);
        while (_queuedAcquisitions.TryDequeue(out var waiter))
        {
            waiter.TrySetException(new ConnectionException(
                "Connection closed while a queued lease acquisition was pending."));
        }
    }

    void EnsureAcquireNotificationHandlerInitialized()
    {
        ThrowIfDisposed();
        if (_registerNotificationHandler is null)
            throw new InvalidOperationException("Notification handlers not configured for queued lease acquisition");
        lock (_gate)
        {
            if (_acquireNotificationHandlerInitialized)
                return;
            _acquireNotificationHandlerInitialized = true;
            _acquireNotificationRegistration = _registerNotificationHandler(MessageTypes.LeaseAcquire, payload =>
            {
                while (_queuedAcquisitions.TryDequeue(out var waiter))
                {
                    if (waiter.TrySetResult(payload.ToArray()))
                        break;
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
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        return await WithLeaseAsync(
            route,
            ttlSecs,
            (_, cancellationToken) => callback(cancellationToken),
            options,
            ct).ConfigureAwait(false);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Lease execution must aggregate arbitrary user callback, renewal, and cleanup failures.")]
    [SuppressMessage("Reliability", "CA2007:Consider calling ConfigureAwait on the awaited task", Justification = "The await-using declaration must retain the strongly typed lease handle for renewal operations.")]
    [SuppressMessage("Reliability", "CA2025:Do not pass IDisposable instances into unawaited tasks", Justification = "The connection-loss observer is stopped and awaited before either cancellation source is disposed.")]
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The renewal deadline is a using declaration and is disposed on every success and failure path.")]
    public async Task<T> WithLeaseAsync<T>(
        string route,
        ulong ttlSecs,
        Func<LeaseAuthority, CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        ct.ThrowIfCancellationRequested();
        if (ttlSecs == 0 || ttlSecs > uint.MaxValue / 1000)
        {
            throw new LeaseException("ttlSecs must be positive and schedulable", "INVALID_TTL");
        }

        var waitSeconds = options?.WaitForAvailability == true ? options.WaitSeconds : 0;
        await using var lease = await AcquireLeaseAsync(route, ttlSecs, waitSeconds, ct).ConfigureAwait(false);
        var authority = new LeaseAuthority(lease.FencingToken);

        using var lifecycle = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var connectionLossObserverStop = new CancellationTokenSource();
        var connectionLossObserver = ObserveConnectionLossAsync(
            lease.ConnectionLost,
            lifecycle,
            connectionLossObserverStop.Token);
        Task<T> callbackTask;
        try
        {
            callbackTask = callback(authority, lifecycle.Token).AsTask();
        }
        catch (Exception error)
        {
            callbackTask = Task.FromException<T>(error);
        }

        Exception? leaseLoss = null;
        Exception? lifecycleCancellationError = null;
        while (!callbackTask.IsCompleted)
        {
            var renewalInterval = TimeSpan.FromSeconds(ttlSecs / 3d);
            var delay = Task.Delay(renewalInterval, lifecycle.Token);
            var completed = await Task.WhenAny(callbackTask, connectionLossObserver, delay).ConfigureAwait(false);
            if (ct.IsCancellationRequested)
            {
                break;
            }
            if (lease.ConnectionLost.IsCompleted)
            {
                leaseLoss = new LeaseException("Lease ownership was lost", "LEASE_LOST");
                lifecycleCancellationError = await connectionLossObserver.ConfigureAwait(false);
                break;
            }

            if (completed == callbackTask)
            {
                break;
            }

            try
            {
                using var renewalDeadline = CancellationTokenSource.CreateLinkedTokenSource(lifecycle.Token);
                var remainingLeaseTime = TimeSpan.FromSeconds(ttlSecs) - renewalInterval;
                var renewalBudget = Min(
                    TimeSpan.FromSeconds(5),
                    Min(Max(renewalInterval, TimeSpan.FromSeconds(2)), remainingLeaseTime));
                renewalDeadline.CancelAfter(renewalBudget);
                await lease.ExtendAsync(ttlSecs, renewalDeadline.Token).ConfigureAwait(false);
                if (lease.FencingTokenChanged.IsCompleted)
                {
                    lease.Invalidate();
                    var rotation = new LeaseException(
                        "Lease renewal changed the callback's fencing authority token",
                        "FENCING_TOKEN_CHANGED");
                    leaseLoss = new LeaseException("Lease ownership was lost", "LEASE_LOST", rotation);
                    lifecycleCancellationError = await CaptureCancellationFailureAsync(lifecycle).ConfigureAwait(false);
                    break;
                }
            }
            catch (Exception error)
            {
                lease.Invalidate();
                leaseLoss = new LeaseException("Lease ownership was lost", "LEASE_LOST", error);
                lifecycleCancellationError = await CaptureCancellationFailureAsync(lifecycle).ConfigureAwait(false);
                break;
            }
        }

        await connectionLossObserverStop.CancelAsync().ConfigureAwait(false);
        var observedConnectionLossCancellationError = await connectionLossObserver.ConfigureAwait(false);
        if (leaseLoss is null && lease.ConnectionLost.IsCompleted)
        {
            leaseLoss = new LeaseException("Lease ownership was lost", "LEASE_LOST");
        }
        lifecycleCancellationError ??= observedConnectionLossCancellationError;

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

        ct.ThrowIfCancellationRequested();
        if (callbackError is not null)
            ExceptionDispatchInfo.Capture(callbackError).Throw();
        if (leaseLoss is not null)
            throw leaseLoss;
        if (lifecycleCancellationError is not null)
            ExceptionDispatchInfo.Capture(lifecycleCancellationError).Throw();
        if (releaseError is not null)
            ExceptionDispatchInfo.Capture(releaseError).Throw();
        return value!;
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Managed lease cancellation must preserve arbitrary callback registration failures for lifecycle aggregation.")]
    static async Task<Exception?> CaptureCancellationFailureAsync(CancellationTokenSource cancellation)
    {
        try
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            return null;
        }
        catch (Exception error)
        {
            return error;
        }
    }

    static async Task<Exception?> ObserveConnectionLossAsync(
        Task connectionLost,
        CancellationTokenSource lifecycle,
        CancellationToken stop)
    {
        var stopped = Task.Delay(Timeout.InfiniteTimeSpan, stop);
        var completed = await Task.WhenAny(connectionLost, stopped).ConfigureAwait(false);
        if (completed != connectionLost)
        {
            return null;
        }

        return await CaptureCancellationFailureAsync(lifecycle).ConfigureAwait(false);
    }

    static TimeSpan Min(TimeSpan left, TimeSpan right) => left <= right ? left : right;

    static TimeSpan Max(TimeSpan left, TimeSpan right) => left >= right ? left : right;

    public async Task WithLeaseAsync(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        await WithLeaseAsync(
            route,
            ttlSecs,
            (_, cancellationToken) => callback(cancellationToken),
            options,
            ct).ConfigureAwait(false);
    }

    public async Task WithLeaseAsync(
        string route,
        ulong ttlSecs,
        Func<LeaseAuthority, CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        await WithLeaseAsync(
            route,
            ttlSecs,
            async (authority, cancellationToken) =>
            {
                await callback(authority, cancellationToken).ConfigureAwait(false);
                return true;
            },
            options,
            ct).ConfigureAwait(false);
    }

    public async Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new LeaseException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = _retryRequest is null
            ? await _request(MessageTypes.LeaseQuery, writer.WrittenMemory, ct).ConfigureAwait(false)
            : await _retryRequest(
                RetryOperations.LeaseQuery,
                MessageTypes.LeaseQuery,
                writer.WrittenMemory,
                ct).ConfigureAwait(false);
        var reader = LeaseWireHelpers.ReadSuccess(response, "QUERY");

        var hasHolder = reader.ReadU8();
        if (hasHolder > 1)
        {
            throw new LeaseException($"QUERY response has invalid holder flag {hasHolder}", "QUERY_INVALID_RESPONSE");
        }

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

    public async Task<LeaseListResult> ListAsync(
        string pattern,
        LeaseListCursor? cursor = null,
        int? limit = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureLeaseRegistrationPattern(pattern);
        if (limit.HasValue)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(limit.Value, 1, nameof(limit));
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        writer.WriteU8((byte)(cursor is null ? 0 : 1));
        if (cursor is not null)
        {
            writer.WriteU64(cursor.SnapshotId);
            writer.WriteU32(cursor.Offset);
        }
        writer.WriteU32((uint)(limit ?? 0));

        var response = await _request(MessageTypes.LeaseList, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = LeaseWireHelpers.ReadSuccess(response, "LIST");
        if (reader.RemainingBytes < 5)
        {
            throw new LeaseException("LIST response is missing its item count or has_next flag", "LIST_INVALID_RESPONSE");
        }

        var itemCount = reader.ReadU32();
        const int minimumItemWireBytes = 4 + 4 + 8 + 4 + 8 + 4;
        var plausibleItemCount = reader.RemainingBytes / minimumItemWireBytes;
        if (itemCount > (uint)plausibleItemCount)
        {
            throw new LeaseException("LIST response item count exceeds the remaining payload", "LIST_INVALID_RESPONSE");
        }

        var items = new List<LeaseListItem>(checked((int)itemCount));
        for (var i = 0; i < itemCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            var route = reader.ReadString();
            var ownerId = reader.ReadString();
            var holderIncarnation = reader.ReadU64();
            var acquiredAt = reader.ReadString();
            var expiresInSecs = reader.ReadU64();
            var renewals = reader.ReadU32();
            items.Add(new LeaseListItem(route, ownerId, holderIncarnation, acquiredAt, expiresInSecs, renewals));
        }

        if (reader.RemainingBytes < 1)
        {
            throw new LeaseException("LIST response missing has_next", "LIST_INVALID_RESPONSE");
        }

        var hasNext = reader.ReadU8();
        if (hasNext > 1)
        {
            throw new LeaseException("LIST response has invalid has_next", "LIST_INVALID_RESPONSE");
        }

        LeaseListCursor? nextCursor = null;
        if (hasNext == 1)
        {
            var snapshotId = reader.ReadU64();
            var offset = reader.ReadU32();
            nextCursor = new LeaseListCursor(snapshotId, offset);
        }

        if (!reader.IsEof)
        {
            throw new LeaseException("LIST response has trailing bytes", "LIST_INVALID_RESPONSE");
        }

        return new LeaseListResult(items, nextCursor);
    }

    public Task<LeaseSubscription> SubscribeAsync(
        string route,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return SubscribeObserverAsync(route, invalidate: null, ct);
    }

    internal async Task<LeaseSubscription> SubscribeObserverAsync(
        string route,
        Action<LeaseChangeEvent>? invalidate,
        CancellationToken ct)
    {
        var buffer = new AsyncSubscriptionBuffer<LeaseChangeEvent>(route, _subscriptionBufferCapacity);
        var registration = await SubscribeAsync(route, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, invalidate, ct).ConfigureAwait(false);
        buffer.ObserveCompletion(registration.Completion);
        return new LeaseSubscription(route, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            await registration.UnsubscribeAsync(token).ConfigureAwait(false);
            buffer.Complete();
        }, registration.Completion);
    }

    internal async Task<LeaseSubscription> SubscribeAsync(
        string route,
        Func<LeaseChangeEvent, CancellationToken, ValueTask> handler,
        Action<LeaseChangeEvent>? preDispatch = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        EnsureLeaseRegistrationPattern(route);

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        EnsureNotificationHandlerInitialized();

        var channel = SubscriptionRegistration<LeaseChangeEvent>.CreateChannel(_subscriptionBufferCapacity);
        SubscriptionRegistration<LeaseChangeEvent>? registration = null;
        var handleId = Interlocked.Increment(ref _nextHandleId);
        var gateAcquired = false;

        try
        {
            registration = new SubscriptionRegistration<LeaseChangeEvent>(
                channel,
                "lease",
                route,
                token => UnsubscribeAsync(route, handleId, token),
                preDispatch);
            await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
            gateAcquired = true;

            lock (_gate)
            {
                if (_subscriptionsByRoute.TryGetValue(route, out var existingSubscription))
                {
                    existingSubscription.Registrations[handleId] = registration;
                    var existingHandle = CreateSubscription(route, handleId, registration.Completion);
                    SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                    registration = null;
                    return existingHandle;
                }
            }

            var subscriptionId = await SubscribeWireAsync(route, ct).ConfigureAwait(false);
            var subscription = new LeaseSubscriptionState(subscriptionId);
            subscription.Registrations[handleId] = registration;
            lock (_gate)
            {
                _subscriptionsByRoute[route] = subscription;
                _routesBySubscriptionId[subscriptionId] = route;
            }

            var handle = CreateSubscription(route, handleId, registration.Completion);
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

    static void EnsureLeaseRegistrationPattern(string pattern)
    {
        if (!RouteValidation.IsRegistrationPattern(pattern, "lease", 3))
        {
            throw new LeaseException($"selector '{pattern}' must be lease://{{realm}}/{{area}}/{{resource}} or a whole-segment wildcard pattern", "INVALID_ROUTE");
        }
    }

    LeaseSubscription CreateSubscription(string route, long handleId, Task completion)
    {
        return new LeaseSubscription(
            route,
            cancellationToken => UnsubscribeAsync(route, handleId, cancellationToken),
            completion);
    }

    async Task<ulong> SubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.LeaseSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = LeaseWireHelpers.ReadSuccess(response, "SUBSCRIBE");

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

    async Task UnsubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.LeaseUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = LeaseWireHelpers.ReadSuccess(response, "UNSUBSCRIBE");
        if (!reader.IsEof)
        {
            throw new LeaseException("UNSUBSCRIBE response has trailing bytes", "UNSUBSCRIBE_INVALID_RESPONSE");
        }
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership intentionally remains in the subscription table when the wire unsubscribe fails.")]
    async ValueTask UnsubscribeAsync(string route, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<LeaseChangeEvent>? registration = null;
        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var shouldUnsubscribe = false;
            lock (_gate)
            {
                if (!_subscriptionsByRoute.TryGetValue(route, out var subscription) ||
                    !subscription.Registrations.TryGetValue(handleId, out registration))
                {
                    return;
                }

                shouldUnsubscribe = subscription.Registrations.Count == 1;
            }

            if (shouldUnsubscribe)
            {
                await UnsubscribeWireAsync(route, ct).ConfigureAwait(false);
            }

            lock (_gate)
            {
                if (!_subscriptionsByRoute.TryGetValue(route, out var subscription) ||
                    !subscription.Registrations.Remove(handleId, out registration))
                {
                    return;
                }

                if (subscription.Registrations.Count == 0)
                {
                    _subscriptionsByRoute.Remove(route);
                    _routesBySubscriptionId.Remove(subscription.SubscriptionId);
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();

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

            _notificationRegistration = _registerNotificationHandler(MessageTypes.LeaseNotify, HandleNotification);
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
            if (reader.ReadU32() != 0 || !reader.IsEof)
            {
                return;
            }
            SubscriptionRegistration<LeaseChangeEvent>[] registrations;
            lock (_gate)
            {
                if (!_routesBySubscriptionId.TryGetValue(subscriptionId, out var registeredRoute) ||
                    !_subscriptionsByRoute.TryGetValue(registeredRoute, out var subscription))
                {
                    return;
                }

                registrations = [.. subscription.Registrations.Values];
            }

            var notification = new LeaseChangeEvent(route);
            foreach (var registration in registrations)
            {
                try
                {
                    registration.PreDispatch?.Invoke(notification);
                    if (!registration.Channel.Writer.TryWrite(notification))
                        _ = registration.FailOverflowAsync();
                }
                catch (Exception exception)
                {
                    _ = registration.FailAsync(exception);
                }
            }
        }
        catch
        {
        }
    }

    async ValueTask HandleReconnect(CancellationToken cancellationToken)
    {
        await RestoreSubscriptionsAsync(cancellationToken).ConfigureAwait(false);

        List<Func<CancellationToken, ValueTask>> listeners;
        lock (_reconnectListenersGate)
        {
            listeners = _reconnectListeners.Count == 0
                ? []
                : [.. _reconnectListeners];
        }

        foreach (var listener in listeners)
        {
            await listener(cancellationToken).ConfigureAwait(false);
        }

        Volatile.Write(ref _acquisitionLanePoisoned, 0);
    }

    /// <summary>
    /// Test-only seam: runs the same reconnect handling (restore subscriptions, then notify
    /// registered reconnect listeners such as <see cref="ILeaseInventoryObserver"/> instances)
    /// that a real broker-side reconnect triggers via <c>FitzConnection.OnReconnect</c>.
    /// </summary>
    internal ValueTask SimulateReconnectAsync(CancellationToken ct = default) => HandleReconnect(ct);

    /// <summary>
    /// Registers a listener that runs after every reconnect-driven subscription restore. Used by
    /// <see cref="ILeaseInventoryObserver"/> to rebuild its view from scratch, since Lease
    /// subscriptions and state do not survive a broker-side disconnect.
    /// </summary>
    internal IDisposable RegisterReconnectListener(Func<CancellationToken, ValueTask> listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        lock (_reconnectListenersGate)
        {
            _reconnectListeners.Add(listener);
        }

        return new ReconnectListenerRegistration(this, listener);
    }

    void RemoveReconnectListener(Func<CancellationToken, ValueTask> listener)
    {
        lock (_reconnectListenersGate)
        {
            _reconnectListeners.Remove(listener);
        }
    }

    public async Task<ILeaseInventoryObserver> ObserveAsync(
        string pattern,
        LeaseObserveOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureLeaseRegistrationPattern(pattern);

        options ??= new LeaseObserveOptions();
        if (options.ReconciliationInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ReconciliationInterval must be positive");
        }
        if (double.IsNaN(options.ReconciliationJitterRatio) ||
            options.ReconciliationJitterRatio < 0 ||
            options.ReconciliationJitterRatio >= 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ReconciliationJitterRatio must be in [0, 1)");
        }
        if (options.UpdateBufferCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "UpdateBufferCapacity must be positive");
        }
        if (options.ListTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "ListTimeout must be positive");
        }

        var observer = new LeaseInventoryObserver(this, pattern, options);
        await observer.StartAsync(ct).ConfigureAwait(false);
        return observer;
    }

    sealed class ReconnectListenerRegistration : IDisposable
    {
        readonly LeaseClient _owner;
        readonly Func<CancellationToken, ValueTask> _listener;
        int _disposed;

        public ReconnectListenerRegistration(LeaseClient owner, Func<CancellationToken, ValueTask> listener)
        {
            _owner = owner;
            _listener = listener;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _owner.RemoveReconnectListener(_listener);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect restoration must best-effort roll back every already-restored subscription before preserving the original failure.")]
    async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
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

            try
            {
                foreach (var entry in snapshot)
                {
                    var subscriptionId = await SubscribeWireAsync(entry.Route, cancellationToken).ConfigureAwait(false);
                    restoredSubscriptions[entry.Route] = entry.Subscription.Clone(subscriptionId);
                    restoredRoutesById[subscriptionId] = entry.Route;
                }
            }
            catch
            {
                foreach (var route in restoredSubscriptions.Keys)
                {
                    try
                    {
                        await UnsubscribeWireAsync(route, CancellationToken.None).ConfigureAwait(false);
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
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _notificationRegistration?.Dispose();
        _acquireNotificationRegistration?.Dispose();
        _acquisitionDisconnectRegistration?.Dispose();
        while (_queuedAcquisitions.TryDequeue(out var waiter))
            waiter.TrySetCanceled();
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

    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    sealed class LeaseSubscriptionState
    {
        public LeaseSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; init; }

        public Dictionary<long, SubscriptionRegistration<LeaseChangeEvent>> Registrations { get; } = [];

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
