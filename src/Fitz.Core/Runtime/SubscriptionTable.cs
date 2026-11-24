using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz.Runtime;

/// <summary>Registers a pattern with the broker and returns its subscription identifier.</summary>
/// <param name="pattern">The route or pattern to subscribe.</param>
/// <param name="session">The session that owns this change, when connected to a broker.</param>
/// <param name="ct">Cancellation token for the request.</param>
/// <returns>The broker-assigned subscription identifier.</returns>
delegate Task<ulong> SubscribeWire(string pattern, SubscriptionSession session, CancellationToken ct);

/// <summary>Withdraws a broker subscription.</summary>
/// <param name="pattern">The route or pattern that was subscribed.</param>
/// <param name="subscriptionId">The identifier the broker assigned to it.</param>
/// <param name="session">The session that owns this change, when connected to a broker.</param>
/// <param name="ct">Cancellation token for the request.</param>
/// <returns>A task that completes once the broker accepts the unsubscribe.</returns>
delegate Task UnsubscribeWire(string pattern, ulong subscriptionId, SubscriptionSession session, CancellationToken ct);

readonly record struct SubscriptionSession(FitzConnection? Connection, CancellationToken Closed)
{
    internal bool IsCurrent => Connection?.IsRequestSessionCurrent(Closed) ?? true;

    internal ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        ushort messageType, ReadOnlyMemory<byte> payload, CancellationToken ct) =>
        Connection is { } connection
            ? connection.RequestOnSessionAsync(messageType, payload, Closed, ct)
            : request(messageType, payload, ct);
}

/// <summary>Decodes one NOTIFY frame for a domain.</summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <param name="payload">The NOTIFY payload.</param>
/// <returns>
/// The decoded notification, or <see langword="null"/> when the frame is malformed and must be dropped.
/// </returns>
/// <remarks>A decoder may also throw <see cref="FitzException"/> for a malformed frame.</remarks>
delegate DecodedNotification<TNotification>? NotificationDecoder<TNotification>(ReadOnlyMemory<byte> payload);

/// <summary>A decoded NOTIFY frame.</summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <param name="SubscriptionId">The broker subscription the frame was delivered for.</param>
/// <param name="Route">The concrete route the notification describes.</param>
/// <param name="Value">The notification delivered to handlers.</param>
readonly record struct DecodedNotification<TNotification>(ulong SubscriptionId, string Route, TNotification Value);

/// <summary>Builds a domain's public subscription handle around a buffered notification stream.</summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <typeparam name="TSubscription">The domain's subscription handle type.</typeparam>
delegate TSubscription BufferedSubscriptionFactory<TNotification, TSubscription>(
    string pattern,
    IAsyncEnumerable<TNotification> notifications,
    Func<CancellationToken, ValueTask> unsubscribe,
    Task completion,
    TimeProvider time);

/// <summary>
/// The subscription bookkeeping every notification domain shares: one broker subscription per
/// pattern, any number of local handles on it, routing of NOTIFY frames to those handles,
/// restoration after reconnect, and ordered shutdown.
/// </summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <remarks>
/// Domains own only what differs between them: pattern validation, the wire encoding of
/// SUBSCRIBE and UNSUBSCRIBE, and the NOTIFY decoder. Changes to a single pattern are ordered,
/// and the broker subscription is withdrawn only when the last local handle leaves. A failed
/// wire unsubscribe keeps the handle registered so the caller can retry.
/// </remarks>
sealed class SubscriptionTable<TNotification> : IAsyncDisposable
{
    readonly object _owner;
    readonly string _domain;
    readonly ushort _notifyMessageType;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly TimeProvider _time;
    readonly SubscribeWire _subscribe;
    readonly UnsubscribeWire _unsubscribe;
    readonly NotificationDecoder<TNotification> _decode;
    readonly NotificationActivationBuffer<TNotification> _activationBuffer;
    readonly int _maxConcurrentChanges;
    readonly SemaphoreSlim _exclusiveGate = new(1, 1);
    readonly HashSet<Task<Exception?>> _changes = [];
    RequestGate _wireGate;
    CancellationToken? _configuredSession;
    TaskCompletionSource? _paused;
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    readonly Lock _gate = new();
    readonly Dictionary<string, PatternSubscription> _byPattern = [with(StringComparer.Ordinal)];
    readonly Dictionary<ulong, PatternSubscription> _byId = [];
    FitzConnection? _connection;
    IDisposable? _notificationRegistration;
    long _nextHandleId;
    bool _disposed;

    /// <summary>Creates the table.</summary>
    /// <param name="owner">The domain client, named in <see cref="ObjectDisposedException"/>.</param>
    /// <param name="domain">Domain name used in diagnostics and overflow errors.</param>
    /// <param name="notifyMessageType">The domain's NOTIFY message type.</param>
    /// <param name="registerNotificationHandler">Connection hook for NOTIFY frames, or <see langword="null"/> when subscriptions are unsupported.</param>
    /// <param name="dispatchAsyncHandler">Shared callback dispatcher, or <see langword="null"/> to run handlers inline.</param>
    /// <param name="bufferCapacity">Per-handle notification buffer size.</param>
    /// <param name="subscribe">Encodes SUBSCRIBE and returns the subscription identifier.</param>
    /// <param name="unsubscribe">Encodes UNSUBSCRIBE.</param>
    /// <param name="decode">Decodes NOTIFY frames.</param>
    /// <param name="timeProvider">The clock for overflow cleanup budgets; the system clock by default.</param>
    /// <param name="maxConcurrentChanges">Upper bound for distinct patterns changing concurrently.</param>
    internal SubscriptionTable(
        object owner,
        string domain,
        ushort notifyMessageType,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler,
        AsyncHandlerDispatch? dispatchAsyncHandler,
        int bufferCapacity,
        SubscribeWire subscribe,
        UnsubscribeWire unsubscribe,
        NotificationDecoder<TNotification> decode,
        TimeProvider? timeProvider = null,
        int maxConcurrentChanges = 1)
    {
        _owner = owner;
        _time = timeProvider ?? TimeProvider.System;
        _domain = domain;
        _notifyMessageType = notifyMessageType;
        _registerNotificationHandler = registerNotificationHandler;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        BufferCapacity = bufferCapacity;
        _subscribe = subscribe;
        _unsubscribe = unsubscribe;
        _decode = decode;
        _maxConcurrentChanges = maxConcurrentChanges;
        _wireGate = new RequestGate(maxConcurrentChanges, ClientConfig.DefaultRequestQueueSize);
        _activationBuffer = new NotificationActivationBuffer<TNotification>(bufferCapacity, maxConcurrentChanges);
        _closingToken = _closing.Token;
    }

    /// <summary>Per-handle notification buffer size.</summary>
    internal int BufferCapacity { get; }

    /// <summary>
    /// Restores subscriptions when <paramref name="connection"/> reconnects and fails them when it
    /// cannot deliver notifications.
    /// </summary>
    /// <param name="connection">The connection the domain client runs on.</param>
    /// <param name="onReconnect">
    /// Replaces the default reconnect handling (<see cref="RestoreAsync"/>) for domains that do more on reconnect.
    /// </param>
    /// <returns>A binding that detaches both hooks when disposed.</returns>
    internal IDisposable AttachTo(FitzConnection connection, Func<CancellationToken, ValueTask>? onReconnect = null)
    {
        _connection = connection;
        _wireGate = new RequestGate(1, connection.MaxRequestQueueSize);
        return new ConnectionBinding(connection, onReconnect ?? RestoreAsync, Fail);
    }

    /// <summary>
    /// Subscribes and exposes notifications as an asynchronous stream wrapped in the domain's handle.
    /// </summary>
    /// <typeparam name="TSubscription">The domain's subscription handle type.</typeparam>
    /// <param name="pattern">A route or pattern the domain has already validated.</param>
    /// <param name="create">Builds the domain handle.</param>
    /// <param name="preDispatch">Optional receive-path hook; see <see cref="SubscribeAsync"/>.</param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>The domain handle.</returns>
    internal async Task<TSubscription> SubscribeBufferedAsync<TSubscription>(
        string pattern,
        BufferedSubscriptionFactory<TNotification, TSubscription> create,
        Action<TNotification>? preDispatch,
        CancellationToken ct)
    {
        var buffer = new AsyncSubscriptionBuffer<TNotification>(pattern, BufferCapacity);
        var binding = await SubscribeAsync(pattern, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, preDispatch, ct).ConfigureAwait(false);
        buffer.ObserveCompletion(binding.Completion);
        return create(pattern, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            await binding.Unsubscribe(token).ConfigureAwait(false);
            buffer.Complete();
        }, binding.Completion, _time);
    }

    /// <summary>The clock that bounds subscription cleanup, for handles built outside the table.</summary>
    internal TimeProvider Time => _time;

    internal async Task<TSubscription> SubscribeReaderAsync<TSubscription>(
        string pattern, BufferedSubscriptionFactory<TNotification, TSubscription> create, CancellationToken ct)
    {
        var (binding, reader) = await SubscribeCoreAsync(pattern, handler: null, preDispatch: null, false, ct).ConfigureAwait(false);
        return create(pattern, reader.ReadAllAsync(CancellationToken.None), binding.Unsubscribe, binding.Completion, _time);
    }

    /// <summary>Subscribes a callback to a pattern.</summary>
    /// <param name="pattern">A route or pattern the domain has already validated.</param>
    /// <param name="handler">Callback invoked for each notification.</param>
    /// <param name="preDispatch">
    /// Optional synchronous hook run on the receive path before the notification is queued. A
    /// <see cref="FitzException"/> it throws faults only this handle.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <param name="bestEffort">Always withdraw the local handle on automatic reader cleanup.</param>
    /// <returns>The handle's unsubscribe callback and completion signal.</returns>
    internal async Task<SubscriptionBinding> SubscribeAsync(
        string pattern,
        Func<TNotification, CancellationToken, ValueTask> handler,
        Action<TNotification>? preDispatch,
        CancellationToken ct, bool bestEffort = false)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var (binding, _) = await SubscribeCoreAsync(pattern, handler, preDispatch, bestEffort, ct).ConfigureAwait(false);
        return binding;
    }

    async Task<(SubscriptionBinding Binding, ChannelReader<TNotification> Reader)> SubscribeCoreAsync(
        string pattern, Func<TNotification, CancellationToken, ValueTask>? handler,
        Action<TNotification>? preDispatch, bool bestEffort, CancellationToken ct)
    {
        EnsureNotificationHandlerRegistered();

        var handleId = Interlocked.Increment(ref _nextHandleId);
        SubscriptionRegistration<TNotification>? registration = new(
            SubscriptionRegistration<TNotification>.CreateChannel(BufferCapacity),
            _domain,
            pattern,
            token => UnsubscribeAsync(pattern, handleId, token),
            preDispatch,
            _time,
            directReader: handler is null);
        ValueTask Unsubscribe(CancellationToken token) => bestEffort
            ? UnsubscribeFollowAsync(pattern, handleId, token)
            : UnsubscribeAsync(pattern, handleId, token);
        var binding = new SubscriptionBinding(Unsubscribe, registration.Completion,
            bestEffort ? registration.CancellationToken : default);
        try
        {
            await AttachHandleAsync(pattern, handleId, registration, ct).ConfigureAwait(false);
            if (handler is not null)
                SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
            var reader = registration.Channel.Reader;
            registration = null;
            return (binding, reader);
        }
        finally
        {
            registration?.Dispose();
            registration?.DisposeCancellationSource();
        }
    }

    async Task AttachHandleAsync(
        string pattern, long handleId, SubscriptionRegistration<TNotification> registration, CancellationToken ct)
    {
        while (true)
        {
            var session = await WaitForReadySessionAsync(ct).ConfigureAwait(false);
            PatternSubscription? group = null;
            TaskCompletionSource<Exception?>? activation = null;
            Task? wait = null;
            var start = false;

            lock (_gate)
            {
                ThrowIfDisposed();
                ct.ThrowIfCancellationRequested();
                if (_paused is { } paused)
                {
                    wait = paused.Task;
                }
                else if (session.IsCurrent)
                {
                    if (!_byPattern.TryGetValue(pattern, out group))
                    {
                        ConfigureWireGate(session);
                        group = new PatternSubscription();
                        _byPattern.Add(pattern, group);
                        StartChange(group);
                        start = true;
                    }

                    if (group.SubscriptionId.HasValue && group.Change is { } removing)
                    {
                        wait = removing.Task;
                    }
                    else
                    {
                        group.Add(handleId, registration);
                        activation = group.Change;
                        if (activation is null)
                            return;
                    }
                }
            }

            if (activation is null)
            {
                if (wait is not null)
                    await wait.WaitAsync(ct).ConfigureAwait(false);
                continue;
            }

            if (start)
                _ = ActivateAsync(pattern, group!, activation, session);

            Exception? error;
            try
            {
                error = await activation.Task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                lock (_gate)
                {
                    // Publication wins a race with cancellation. A pending caller can withdraw
                    // independently while the operation still owns the response and any cleanup.
                    if (activation.Task.IsCompletedSuccessfully && group!.SubscriptionId.HasValue &&
                        group.Failure is null && group.Contains(handleId))
                        return;
                    group!.Remove(handleId, out _);
                }
                throw;
            }

            if (error is not null)
                ExceptionDispatchInfo.Throw(error);
            return;
        }
    }

    async Task ActivateAsync(
        string pattern, PatternSubscription group, TaskCompletionSource<Exception?> change, SubscriptionSession session)
    {
        Exception? failure = null;
        var started = false;
        try
        {
            using var slot = await _wireGate.AcquireAsync(_closingToken).ConfigureAwait(false);
            lock (_gate)
            {
                EnsureActivationCurrent(group, session);
                if (group.Count == 0)
                {
                    RemoveGroup(pattern, group);
                    return;
                }

                _activationBuffer.Begin(pattern);
                started = true;
            }

            var id = await _subscribe(pattern, session, _closingToken).ConfigureAwait(false);

            lock (_gate)
            {
                EnsureActivationCurrent(group, session);
                group.SubscriptionId = id;
                var error = _activationBuffer.Take(id, out var frames);
                if (group.Count > 0)
                {
                    _byId.Add(id, group);
                    var handles = group.Snapshot;
                    if (error is not null)
                    {
                        foreach (var handle in handles)
                            _ = handle.FailAsync(error);
                    }
                    else if (frames is not null)
                    {
                        foreach (var frame in frames)
                            Deliver(handles, frame);
                    }

                    _activationBuffer.End(pattern);
                    FinishChange(group, change, error: null);
                    return;
                }

                // A reply can arrive after all callers canceled. Keep the removal in this
                // selector's change so a new caller cannot acquire the ID being withdrawn.
            }

            await WithdrawAbandonedAsync(pattern, id, session).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            failure = _closingToken.IsCancellationRequested ? new ObjectDisposedException(_owner.GetType().Name) : error;
        }
        finally
        {
            lock (_gate)
            {
                if (!change.Task.IsCompleted)
                {
                    if (started)
                        _activationBuffer.End(pattern);
                    RemoveGroup(pattern, group);
                    FinishChange(group, change, group.Failure ?? failure);
                }
            }
        }
    }

    void EnsureActivationCurrent(PatternSubscription group, SubscriptionSession session)
    {
        ThrowIfDisposed();
        if (group.Failure is { } error)
            ExceptionDispatchInfo.Throw(error);
        if (!session.IsCurrent)
            throw new ConnectionException("The subscription response belongs to an ended session.");
    }

    async Task WithdrawAbandonedAsync(string pattern, ulong id, SubscriptionSession session)
    {
        if (!session.IsCurrent)
            return;

        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5), _time);
        try
        {
            await _unsubscribe(pattern, id, session, cleanup.Token).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            session.Connection?.InvalidateSubscriptionSession(error, session.Closed);
        }
    }

    /// <summary>
    /// Runs a broker request that drops every subscription at once, then ends every local handle.
    /// </summary>
    /// <param name="request">The domain's wire request.</param>
    /// <param name="ct">Cancellation token for the request.</param>
    /// <returns>A task that completes once the broker accepts the request.</returns>
    internal async Task RemoveAllAsync(Func<SubscriptionSession, CancellationToken, Task> request, CancellationToken ct)
    {
        var acquired = await EnterExclusiveOnReadySessionAsync(ct).ConfigureAwait(false);
        try
        {
            await DrainChangesAsync(ct).ConfigureAwait(false);
            await request(acquired, ct).ConfigureAwait(false);
            foreach (var registration in DetachAll())
                registration.Dispose();
        }
        finally
        {
            EndExclusive();
        }
    }

    /// <summary>
    /// Re-establishes every broker subscription on a new session. On failure, rolls back the
    /// subscriptions it re-created and keeps the previous state so a later reconnect can retry.
    /// </summary>
    /// <param name="ct">Cancellation token for the restore requests.</param>
    /// <returns>A task that completes once every subscription is restored.</returns>
    internal async ValueTask RestoreAsync(CancellationToken ct)
    {
        if (!await _exclusiveGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            return;

        try
        {
            await DrainChangesAsync(ct).ConfigureAwait(false);
            var session = CurrentSession();
            KeyValuePair<string, PatternSubscription>[] snapshot;
            lock (_gate)
            {
                snapshot = [.. _byPattern];
            }

            var restored = new ulong?[snapshot.Length];
            var degree = WireConcurrency(session);
            async ValueTask RestoreOneAsync(int index, CancellationToken token) =>
                restored[index] = await _subscribe(snapshot[index].Key, session, token).ConfigureAwait(false);

            try
            {
                if (degree == 1)
                {
                    for (var i = 0; i < snapshot.Length; i++)
                        await RestoreOneAsync(i, ct).ConfigureAwait(false);
                }
                else
                {
                    await Parallel.ForEachAsync(Enumerable.Range(0, snapshot.Length),
                        new ParallelOptions { MaxDegreeOfParallelism = degree, CancellationToken = ct },
                        RestoreOneAsync).ConfigureAwait(false);
                }
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
                // The failed connected attempt will close its transport and release all IDs.
                if (_connection is null)
                {
                    for (var i = 0; i < snapshot.Length; i++)
                    {
                        if (restored[i] is { } id)
                            await WithdrawAbandonedAsync(snapshot[i].Key, id, session).ConfigureAwait(false);
                    }
                }
                throw;
            }

            List<(string Pattern, ulong Id)>? withdrawn = null;
            lock (_gate)
            {
                if (!session.IsCurrent)
                    throw new ConnectionException("The subscription restore belongs to an ended session.");
                _byId.Clear();
                for (var i = 0; i < snapshot.Length; i++)
                {
                    var (pattern, group) = snapshot[i];
                    var id = restored[i].GetValueOrDefault();
                    if (_byPattern.TryGetValue(pattern, out var current) && ReferenceEquals(current, group))
                    {
                        group.SubscriptionId = id;
                        _byId.Add(id, group);
                    }
                    else
                    {
                        // The reader left while SUBSCRIBE was in flight.
                        (withdrawn ??= []).Add((pattern, id));
                    }
                }
            }

            if (withdrawn is not null)
            {
                foreach (var (pattern, id) in withdrawn)
                    await _unsubscribe(pattern, id, session, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            EndExclusive();
        }
    }

    /// <summary>
    /// Ends every handle with <paramref name="error"/> after the connection could not deliver
    /// notifications. The handles cannot be restored; callers must subscribe again.
    /// </summary>
    /// <param name="error">The delivery failure.</param>
    internal void Fail(Exception error)
    {
        SubscriptionRegistration<TNotification>[] handles;
        lock (_gate)
        {
            foreach (var group in _byPattern.Values)
                group.Failure = error;
            _activationBuffer.Clear();
            handles = DetachAllUnsafe();
        }
        foreach (var handle in handles)
            _ = handle.FailAsync(error);
    }

    /// <summary>Ends local handles and waits for in-flight subscription changes to settle.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, true))
            return;

        await _closing.CancelAsync().ConfigureAwait(false);
        _notificationRegistration?.Dispose();
        await _exclusiveGate.DrainAndDisposeAsync().ConfigureAwait(false);
        await DrainChangesAsync(CancellationToken.None).ConfigureAwait(false);
        foreach (var registration in DetachAll())
            registration.Dispose();
        _closing.Dispose();
    }

    async ValueTask UnsubscribeFollowAsync(string pattern, long handleId, CancellationToken ct)
    {
        try
        {
            await UnsubscribeAsync(pattern, handleId, ct).ConfigureAwait(false);
        }
        finally
        {
            // An ended reader must never be restored, even when its wire cleanup fails.
            SubscriptionRegistration<TNotification>? removed = null;
            lock (_gate)
            {
                if (_byPattern.TryGetValue(pattern, out var group) &&
                    group.Remove(handleId, out removed) && group.Count == 0)
                    RemoveGroup(pattern, group);
            }
            removed?.Dispose();
        }
    }

    async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        while (!_closingToken.IsCancellationRequested)
        {
            ct.ThrowIfCancellationRequested();
            var session = CurrentSession();
            PatternSubscription? group;
            TaskCompletionSource<Exception?>? removal = null;
            SubscriptionRegistration<TNotification>? detached = null;
            Task? wait = null;

            try
            {
                lock (_gate)
                {
                    if (Volatile.Read(ref _disposed) || !_byPattern.TryGetValue(pattern, out group) || !group.Contains(handleId))
                        return;
                    if (_paused is { } paused)
                        wait = paused.Task;
                    else if (group.Change is { } change)
                        wait = change.Task;
                    else if (group.Count > 1)
                        group.Remove(handleId, out detached);
                    else if (session.IsCurrent)
                    {
                        ConfigureWireGate(session);
                        removal = StartChange(group);
                    }
                }

                if (detached is not null)
                    return;
            }
            finally
            {
                detached?.Dispose();
            }

            if (removal is not null)
            {
                await RemoveLastHandleAsync(pattern, group, handleId, removal, session, ct).ConfigureAwait(false);
                return;
            }

            if (wait is not null)
                await wait.WaitAsync(ct).ConfigureAwait(false);
            else
                _ = await WaitForReadySessionAsync(ct).ConfigureAwait(false);
        }
    }

    async Task RemoveLastHandleAsync(string pattern, PatternSubscription group, long handleId,
        TaskCompletionSource<Exception?> change, SubscriptionSession session, CancellationToken ct)
    {
        SubscriptionRegistration<TNotification>? removed = null;
        try
        {
            using var queued = CancellationTokenSource.CreateLinkedTokenSource(ct, _closingToken);
            using var slot = await _wireGate.AcquireAsync(queued.Token).ConfigureAwait(false);
            queued.Token.ThrowIfCancellationRequested();
            await _unsubscribe(pattern, group.SubscriptionId.GetValueOrDefault(), session, ct).ConfigureAwait(false);
            lock (_gate)
            {
                if (group.Remove(handleId, out removed))
                    RemoveGroup(pattern, group);
            }
        }
        catch (OperationCanceledException) when (_closingToken.IsCancellationRequested && !ct.IsCancellationRequested)
        {
        }
        finally
        {
            lock (_gate)
                FinishChange(group, change, error: null);
            removed?.Dispose();
        }
    }

    SubscriptionSession CurrentSession() => new(_connection, _connection?.ConnectionClosedToken ?? default);

    async ValueTask<SubscriptionSession> WaitForReadySessionAsync(CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_connection is { State: not ConnectionState.Authenticated } connection)
        {
            using var wait = CancellationTokenSource.CreateLinkedTokenSource(_closingToken, ct);
            try
            {
                await connection.WaitForRequestReadyAsync(wait.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_closingToken.IsCancellationRequested && !ct.IsCancellationRequested)
            {
                ThrowIfDisposed();
                throw;
            }
        }
        return CurrentSession();
    }

    async ValueTask<SubscriptionSession> EnterExclusiveOnReadySessionAsync(CancellationToken ct)
    {
        while (true)
        {
            var session = await WaitForReadySessionAsync(ct).ConfigureAwait(false);
            ObjectDisposedException.ThrowIf(!await _exclusiveGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false), _owner);
            if (session.IsCurrent)
                return session;
            _exclusiveGate.Release();
        }
    }

    async Task DrainChangesAsync(CancellationToken ct)
    {
        Task[] changes;
        lock (_gate)
        {
            _paused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            changes = [.. _changes];
        }
        await Task.WhenAll(changes).WaitAsync(ct).ConfigureAwait(false);
    }

    void EndExclusive()
    {
        lock (_gate)
        {
            _paused?.SetResult();
            _paused = null;
        }
        _exclusiveGate.Release();
    }

    int WireConcurrency(SubscriptionSession session) =>
        session.Connection?.SubscriptionRequestConcurrency(_maxConcurrentChanges) ?? _maxConcurrentChanges;

    // Change tracking and group membership are protected by _gate. Completion tasks always
    // resolve to an error value so an exclusive operation can drain changes that failed.
    void ConfigureWireGate(SubscriptionSession session)
    {
        if (_configuredSession != session.Closed)
        {
            _wireGate.SetMaxConcurrency(WireConcurrency(session));
            _configuredSession = session.Closed;
        }
    }

    TaskCompletionSource<Exception?> StartChange(PatternSubscription group)
    {
        var change = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        group.Change = change;
        _changes.Add(change.Task);
        return change;
    }

    void FinishChange(PatternSubscription group, TaskCompletionSource<Exception?> change, Exception? error)
    {
        group.Change = null;
        _changes.Remove(change.Task);
        change.SetResult(error);
    }

    void RemoveGroup(string pattern, PatternSubscription group)
    {
        if (_byPattern.TryGetValue(pattern, out var current) && ReferenceEquals(current, group))
            _byPattern.Remove(pattern);
        if (group.SubscriptionId is { } id && _byId.TryGetValue(id, out current) && ReferenceEquals(current, group))
            _byId.Remove(id);
    }

    void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        DecodedNotification<TNotification> decoded;
        try
        {
            if (_decode(payload) is not { } result)
            {
                FitzDiagnostics.RecordDroppedNotification(_domain, "malformed");
                return;
            }

            decoded = result;
        }
        catch (FitzException)
        {
            FitzDiagnostics.RecordDroppedNotification(_domain, "malformed");
            return;
        }

        SubscriptionRegistration<TNotification>[] registrations;
        lock (_gate)
        {
            if (!_byId.TryGetValue(decoded.SubscriptionId, out var group))
            {
                _activationBuffer.Buffer(decoded);
                return;
            }
            registrations = group.Snapshot;
        }
        Deliver(registrations, decoded.Value);
    }

    static void Deliver(SubscriptionRegistration<TNotification>[] registrations, TNotification value)
    {
        foreach (var registration in registrations)
        {
            if (registration.IsDisposed)
                continue;
            try
            {
                registration.PreDispatch?.Invoke(value);
            }
            catch (FitzException error)
            {
                _ = registration.FailAsync(error);
                continue;
            }

            if (!registration.Channel.Writer.TryWrite(value))
                _ = registration.FailOverflowAsync();
        }
    }

    void EnsureNotificationHandlerRegistered()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_notificationRegistration is not null)
                return;
            if (_registerNotificationHandler is null)
                throw new InvalidOperationException("Notification handlers not configured for subscription support");
            _notificationRegistration = _registerNotificationHandler(_notifyMessageType, HandleNotification);
        }
    }

    SubscriptionRegistration<TNotification>[] DetachAll()
    {
        lock (_gate)
        {
            return DetachAllUnsafe();
        }
    }

    // Callers hold _gate.
    SubscriptionRegistration<TNotification>[] DetachAllUnsafe()
    {
        var registrations = _byPattern.Values.SelectMany(static subscription => subscription.Snapshot).ToArray();
        _byPattern.Clear();
        _byId.Clear();
        return registrations;
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), _owner);

    sealed class ConnectionBinding : IDisposable
    {
        readonly FitzConnection _connection;
        readonly Action<Exception> _onDeliveryFailed;
        readonly IDisposable _reconnect;
        bool _disposed;

        internal ConnectionBinding(FitzConnection connection, Func<CancellationToken, ValueTask> onReconnect, Action<Exception> onDeliveryFailed)
        {
            _connection = connection;
            _onDeliveryFailed = onDeliveryFailed;
            connection.NotificationDeliveryFailed += onDeliveryFailed;
            _reconnect = connection.OnReconnect(onReconnect);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, true))
                return;
            _connection.NotificationDeliveryFailed -= _onDeliveryFailed;
            _reconnect.Dispose();
        }
    }

    /// <summary>One broker subscription and the local handles sharing it.</summary>
    sealed class PatternSubscription
    {
        readonly Dictionary<long, SubscriptionRegistration<TNotification>> _handles = [];
        SubscriptionRegistration<TNotification>[]? _snapshot;

        internal ulong? SubscriptionId { get; set; }
        internal TaskCompletionSource<Exception?>? Change { get; set; }
        internal Exception? Failure { get; set; }
        internal int Count => _handles.Count;
        internal SubscriptionRegistration<TNotification>[] Snapshot => _snapshot ??= [.. _handles.Values];

        internal bool Contains(long handleId) => _handles.ContainsKey(handleId);

        internal void Add(long handleId, SubscriptionRegistration<TNotification> registration)
        {
            _handles.Add(handleId, registration);
            _snapshot = null;
        }

        internal bool Remove(long handleId, out SubscriptionRegistration<TNotification>? registration)
        {
            _snapshot = null;
            return _handles.Remove(handleId, out registration);
        }
    }
}
