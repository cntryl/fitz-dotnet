using System.Globalization;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Lease;

/// <summary>
/// The default <see cref="ILeaseClient"/>: distributed leases, including managed lease scopes.
/// </summary>
/// <remarks>
/// Obtained from <see cref="FitzClient"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class LeaseClient : ILeaseClient, IAsyncDisposable
{
    // ACQUIRE response_type values (lease spec, "ACQUIRE Response").
    const byte AcquireResponseAcquired = 0;
    const byte AcquireResponseAlreadyHeld = 1;
    const byte AcquireResponseQueued = 2;
    const byte AcquireResponseAlreadyQueued = 3;

    const uint MaxAcquireWaitSeconds = 30;

    // Slack past wait_seconds before a queued acquisition with no final frame is abandoned.
    static readonly TimeSpan DeferredGrantGrace = TimeSpan.FromSeconds(5);

    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    readonly Func<Action, IDisposable>? _registerOnDisconnect;
    readonly FitzConnection? _connection;
    readonly TimeProvider _time;
    bool _disposed;
    readonly SubscriptionTable<LeaseChangeEvent> _subscriptions;
    readonly IDisposable? _connectionBinding;
    readonly ListenerRegistry<Func<CancellationToken, ValueTask>> _reconnectListeners = new();

    internal LeaseClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.OnDisconnect,
            (handler, rejected) => connection.TryDispatchAsyncHandler("lease", handler, rejected),
            connection.RequestWithRetryAsync,
            connection.SubscriptionBufferCapacity,
            connection.Time)
    {
        _connection = connection;
        _connectionBinding = _subscriptions.AttachTo(connection, HandleReconnect);
    }

    /// <summary>
    /// Creates a domain client over a request delegate, for testing without a broker.
    /// </summary>
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
        TimeProvider? timeProvider = null)
    {
        _request = request;
        _time = timeProvider ?? TimeProvider.System;
        _registerOnDisconnect = registerOnDisconnect;
        _retryRequest = retryRequest;
        _subscriptions = new SubscriptionTable<LeaseChangeEvent>(
            this,
            "lease",
            MessageTypes.LeaseNotify,
            registerNotificationHandler,
            dispatchAsyncHandler,
            subscriptionBufferCapacity,
            SubscribeWireAsync,
            UnsubscribeWireAsync,
            DecodeNotification,
            _time);
    }

    /// <inheritdoc />
    public async Task<ILease> AcquireAsync(string route, TimeSpan ttl, TimeSpan wait = default, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return await AcquireLeaseAsync(
            route,
            WireDuration.ToSeconds(ttl, nameof(ttl)),
            ToWaitSeconds(wait, nameof(wait)),
            ct).ConfigureAwait(false);
    }

    async Task<LeaseHandle> AcquireLeaseAsync(string route, ulong ttlSecs, uint waitSeconds, CancellationToken ct)
    {
        if (ttlSecs == 0 || ttlSecs > uint.MaxValue / 1000)
        {
            throw new LeaseException("ttlSecs must be positive and schedulable", "INVALID_TTL");
        }

        return await AcquireCoreAsync(route, ttlSecs, waitSeconds, ct).ConfigureAwait(false);
    }

    // The broker caps an ACQUIRE wait at 30 seconds; reject longer waits rather than truncate.
    static uint ToWaitSeconds(TimeSpan wait, string paramName)
    {
        var seconds = WireDuration.ToSecondsUInt32(wait, paramName);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(seconds, MaxAcquireWaitSeconds, paramName);
        return seconds;
    }

    // A contended ACQUIRE answers Queued on a non-final frame, then the grant, timeout, or
    // error on the final frame of the same correlation. The broker bounds the wait; the guard
    // only covers a broker that never sends the final frame.
    async Task<LeaseHandle> AcquireCoreAsync(string route, ulong ttlSecs, uint waitSeconds, CancellationToken ct)
    {
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new ArgumentException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", nameof(route));
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU64(ttlSecs);
        writer.WriteU32(waitSeconds);

        using var session = new SessionRequestContext(_connection, _request, _registerOnDisconnect);
        using var waitGuard = waitSeconds > 0
            ? new CancellationTokenSource(TimeSpan.FromSeconds(waitSeconds) + DeferredGrantGrace, _time)
            : null;
        using var acquisition = waitGuard is null ? null : CancellationTokenSource.CreateLinkedTokenSource(ct, waitGuard.Token);
        ReadOnlyMemory<byte> response;
        try
        {
            response = await session.RequestAsync(MessageTypes.LeaseAcquire, writer.WrittenMemory, acquisition?.Token ?? ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (waitGuard is { IsCancellationRequested: true } && !ct.IsCancellationRequested)
        {
            throw new RequestTimeoutException(
                $"Queued lease acquisition received no final response within its {waitSeconds}-second wait", exception);
        }

        var reader = LeaseWireHelpers.ReadSuccess(response, "ACQUIRE");
        if (reader.RemainingBytes < 9)
        {
            throw new LeaseException("ACQUIRE response missing fencing token", "MISSING_TOKEN");
        }

        var responseType = reader.ReadU8();
        var fencedToken = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new LeaseException("ACQUIRE response has trailing bytes", "ACQUIRE_INVALID_RESPONSE");
        }

        switch (responseType)
        {
            case AcquireResponseAcquired:
            case AcquireResponseAlreadyHeld:
                break;
            case AcquireResponseAlreadyQueued:
                throw new LeaseException("ACQUIRE request is already queued", "ALREADY_QUEUED");
            case AcquireResponseQueued:
                throw new LeaseException("ACQUIRE answered Queued on a final frame", "ACQUIRE_INVALID_RESPONSE");
            default:
                throw new LeaseException($"ACQUIRE response type {responseType} is unknown", "ACQUIRE_INVALID_RESPONSE");
        }

        session.EnsureValid();
        return new LeaseHandle(session.BoundRequest, route, fencedToken, session.RegisterOnDisconnect, _time);
    }

    /// <inheritdoc />
    public async Task<T> WithLeaseAsync<T>(
        string route,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        return await WithLeaseAsync(
            route,
            ttl,
            (_, ct) => callback(ct),
            options,
            ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<T> WithLeaseAsync<T>(
        string route,
        TimeSpan ttl,
        Func<LeaseAuthority, CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        ct.ThrowIfCancellationRequested();
        var ttlSecs = WireDuration.ToSeconds(ttl, nameof(ttl));
        if (ttlSecs == 0 || ttlSecs > uint.MaxValue / 1000)
        {
            throw new LeaseException("ttl must be positive and schedulable", "INVALID_TTL");
        }

        var waitSeconds = options?.WaitForAvailability == true
            ? ToWaitSeconds(options.Wait, nameof(LeaseExecutionOptions.Wait))
            : 0;
        var lease = await AcquireLeaseAsync(route, ttlSecs, waitSeconds, ct).ConfigureAwait(false);
        return await LeaseScope.RunAsync(lease, ttl, callback, _time, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WithLeaseAsync(
        string route,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        await WithLeaseAsync(
            route,
            ttl,
            (_, ct) => callback(ct),
            options,
            ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WithLeaseAsync(
        string route,
        TimeSpan ttl,
        Func<LeaseAuthority, CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(callback);
        await WithLeaseAsync(
            route,
            ttl,
            async (authority, ct) =>
            {
                await callback(authority, ct).ConfigureAwait(false);
                return true;
            },
            options,
            ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "lease", 3))
        {
            throw new ArgumentException($"route '{route}' must be lease://{{realm}}/{{area}}/{{resource}}", nameof(route));
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

        if (!WireDuration.TryFromSeconds(ttlRemaining, out var remaining))
        {
            throw new LeaseException("QUERY response ttl_remaining is out of range", "QUERY_INVALID_RESPONSE");
        }

        return new LeaseInfo(true, owner, remaining, heldPendingWaiters);
    }

    /// <inheritdoc />
    public async Task<LeaseListResult> ListAsync(
        string pattern,
        LeaseListCursor? cursor = null,
        int? limit = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureLeaseRegistrationPattern(pattern, nameof(pattern));
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
            var acquiredAtText = reader.ReadString();
            var expiresInSecs = reader.ReadU64();
            var renewals = reader.ReadU32();
            if (!WireDuration.TryFromSeconds(expiresInSecs, out var expiresIn))
            {
                throw new LeaseException("LIST response expires_in_secs is out of range", "LIST_INVALID_RESPONSE");
            }

            // The broker reports acquired_at as an RFC 3339 timestamp.
            if (!DateTimeOffset.TryParse(acquiredAtText, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var acquiredAt))
            {
                throw new LeaseException("LIST response acquired_at is not an RFC 3339 timestamp", "LIST_INVALID_RESPONSE");
            }

            items.Add(new LeaseListItem(route, ownerId, holderIncarnation, acquiredAt, expiresIn, renewals));
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

    /// <inheritdoc />
    public Task<LeaseSubscription> SubscribeAsync(
        string route,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return SubscribeObserverAsync(route, invalidate: null, ct);
    }

    internal Task<LeaseSubscription> SubscribeObserverAsync(
        string route,
        Action<LeaseChangeEvent>? invalidate,
        CancellationToken ct)
    {
        EnsureLeaseRegistrationPattern(route, nameof(route));
        return _subscriptions.SubscribeBufferedAsync(
            route,
            static (route, notifications, unsubscribe, completion, time) => new LeaseSubscription(route, notifications, unsubscribe, completion, time),
            invalidate,
            ct);
    }

    internal async Task<LeaseSubscription> SubscribeAsync(
        string route,
        Func<LeaseChangeEvent, CancellationToken, ValueTask> handler,
        Action<LeaseChangeEvent>? preDispatch = null,
        CancellationToken ct = default)
    {
        EnsureLeaseRegistrationPattern(route, nameof(route));
        var binding = await _subscriptions.SubscribeAsync(route, handler, preDispatch, ct).ConfigureAwait(false);
        return new LeaseSubscription(route, binding.Unsubscribe, binding.Completion, _subscriptions.Time);
    }

    static void EnsureLeaseRegistrationPattern(string pattern, string paramName)
    {
        if (!RouteValidation.IsRegistrationPattern(pattern, "lease", 3))
        {
            throw new ArgumentException($"selector '{pattern}' must be lease://{{realm}}/{{area}}/{{resource}} or a whole-segment wildcard pattern", paramName);
        }
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

    async Task UnsubscribeWireAsync(string route, ulong subscriptionId, CancellationToken ct)
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

    static DecodedNotification<LeaseChangeEvent>? DecodeNotification(ReadOnlyMemory<byte> payload)
    {
        var reader = new BinaryBufferReader(payload);
        var subscriptionId = reader.ReadU64();
        var route = reader.ReadString();
        return reader.ReadU32() == 0 && reader.IsEof
            ? new DecodedNotification<LeaseChangeEvent>(subscriptionId, route, new LeaseChangeEvent(route))
            : null;
    }

    async ValueTask HandleReconnect(CancellationToken ct)
    {
        await _subscriptions.RestoreAsync(ct).ConfigureAwait(false);

        foreach (var listener in _reconnectListeners.Snapshot())
        {
            await listener(ct).ConfigureAwait(false);
        }
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
    internal IDisposable RegisterReconnectListener(Func<CancellationToken, ValueTask> listener) => _reconnectListeners.Add(listener);

    /// <inheritdoc />
    public async Task<ILeaseInventoryObserver> ObserveAsync(
        string pattern,
        LeaseObserveOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsureLeaseRegistrationPattern(pattern, nameof(pattern));

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

        var observer = new LeaseInventoryObserver(this, pattern, options, _time);
        await observer.StartAsync(ct).ConfigureAwait(false);
        return observer;
    }

    /// <summary>Releases local resources and ends any registrations this client owns.</summary>
    /// <returns>A task that completes once an in-progress subscription change has finished.</returns>
    /// <remarks>Queued and later subscription changes fail with <see cref="ObjectDisposedException"/>;
    /// unsubscribes and reconnect restoration become no-ops.</remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, true))
        {
            return;
        }

        _connectionBinding?.Dispose();
        await _subscriptions.DisposeAsync().ConfigureAwait(false);
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
}
