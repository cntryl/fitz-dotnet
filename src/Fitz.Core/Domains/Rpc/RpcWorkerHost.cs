using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Rpc;

/// <summary>
/// The worker side of RPC: registers local workers with the broker, routes inbound requests to
/// the most specific matching worker within its concurrency limit, and handles cancellation.
/// </summary>
/// <remarks>
/// An inbound request moves through three states, all tracked under one lock: <em>queued</em>
/// (received, waiting for the dispatcher), <em>claimed</em> (taken by the dispatcher, not yet
/// running), and <em>active</em> (running, cancellable). A broker cancellation removes a queued
/// request, marks a claimed one so it starts cancelled, and cancels an active one. Every request
/// the dispatcher took ends with a cleanup acknowledgement to the broker.
/// </remarks>
sealed class RpcWorkerHost : IDisposable
{
    readonly object _owner;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable> _registerNotificationHandler;
    readonly Action _ensureLifecycleHandler;
    readonly Func<Func<CancellationToken, ValueTask>, IDisposable>? _onReconnect;
    readonly Func<CancellationToken>? _getConnectionClosedToken;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly Action<Exception>? _onWorkerError;
    readonly TimeProvider _time;

    readonly object _sync = new();
    readonly Dictionary<string, LocalWorker> _workers = [with(StringComparer.Ordinal)];
    readonly Dictionary<Guid, QueuedInvocation> _queued = [];
    readonly HashSet<Guid> _claimed = [];
    readonly HashSet<Guid> _cancelledBeforeStart = [];
    readonly Dictionary<Guid, CancellationTokenSource> _active = [];
    TaskCompletionSource _availability = new(TaskCreationOptions.RunContinuationsAsynchronously);
    int _waitingCount;
    IDisposable? _requestRegistration;
    IDisposable? _reconnectRegistration;
    bool _disposed;

    internal RpcWorkerHost(
        object owner,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable> registerNotificationHandler,
        Action ensureLifecycleHandler,
        Func<Func<CancellationToken, ValueTask>, IDisposable>? onReconnect,
        Func<CancellationToken>? getConnectionClosedToken,
        AsyncHandlerDispatch? dispatchAsyncHandler,
        Action<Exception>? onWorkerError,
        TimeProvider time)
    {
        _time = time;
        _owner = owner;
        _request = request;
        _send = send;
        _registerNotificationHandler = registerNotificationHandler;
        _ensureLifecycleHandler = ensureLifecycleHandler;
        _onReconnect = onReconnect;
        _getConnectionClosedToken = getConnectionClosedToken;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _onWorkerError = onWorkerError;
    }

    /// <summary>Registers a worker for a validated pattern.</summary>
    internal async Task<RpcWorkerRegistration> RegisterAsync(
        string pattern,
        Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> handler,
        uint maxConcurrency,
        CancellationToken ct)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, _owner);
            if (_workers.ContainsKey(pattern))
                throw new RpcException($"worker pattern '{pattern}' is already registered", "ALREADY_REGISTERED");

            _workers[pattern] = new LocalWorker(handler, maxConcurrency);
            EnsureRequestHandlerLocked();
            NotifyAvailabilityLocked();
        }

        try
        {
            await SubscribeWireAsync(pattern, maxConcurrency, ct).ConfigureAwait(false);
        }
        catch
        {
            lock (_sync)
                RemoveLocked(pattern);
            throw;
        }

        lock (_sync)
            _reconnectRegistration ??= _onReconnect?.Invoke(ResubscribeAsync);

        return new RpcWorkerRegistration(pattern, token => new ValueTask(UnregisterAsync(pattern, token)));
    }

    /// <summary>Applies a broker cancellation to the request with <paramref name="invocationId"/>.</summary>
    internal void Cancel(Guid invocationId)
    {
        CancellationTokenSource? active = null;
        QueuedInvocation? discarded = null;
        lock (_sync)
        {
            if (_active.TryGetValue(invocationId, out active))
            {
            }
            else if (_claimed.Contains(invocationId))
            {
                _cancelledBeforeStart.Add(invocationId);
            }
            else if (_queued.Remove(invocationId, out discarded))
            {
                discarded.Request = null;
            }
        }

        if (active is not null)
            _ = CancelAsync(active);
        if (discarded is not null)
            _ = SendCleanupAckAsync(discarded.CorrelationId, discarded.ConnectionClosed);
    }

    /// <summary>Drops every worker and cancels running invocations.</summary>
    public void Dispose()
    {
        CancellationTokenSource[] active;
        lock (_sync)
        {
            if (_disposed)
                return;
            _disposed = true;
            _requestRegistration?.Dispose();
            _requestRegistration = null;
            _reconnectRegistration?.Dispose();
            _reconnectRegistration = null;
            _workers.Clear();
            active = [.. _active.Values];
            _active.Clear();
            _cancelledBeforeStart.Clear();
            _claimed.Clear();
            foreach (var queued in _queued.Values)
                queued.Request = null;
            _queued.Clear();
        }

        foreach (var cancellation in active)
            _ = CancelAsync(cancellation);
    }

    void EnsureRequestHandlerLocked()
    {
        if (_requestRegistration is not null)
            return;

        _ensureLifecycleHandler();
        _requestRegistration = _registerNotificationHandler(MessageTypes.RpcRequest, OnRequest);
    }

    void OnRequest(ReadOnlyMemory<byte> payload)
    {
        var receivedAt = _time.GetTimestamp();
        var connectionClosed = _getConnectionClosedToken?.Invoke() ?? CancellationToken.None;
        if (RpcWire.TryDecodeRequest(payload) is not { } request)
            return;

        var queued = new QueuedInvocation(request, receivedAt, connectionClosed);
        lock (_sync)
        {
            if (FindWorkerLocked(request.Route, requireCapacity: false) is null || !_queued.TryAdd(queued.Id, queued))
                return;
        }

        if (_dispatchAsyncHandler is null)
        {
            _ = DispatchAsync(queued, reject: false, CancellationToken.None);
            return;
        }

        if (!_dispatchAsyncHandler(
            token => new ValueTask(DispatchAsync(queued, reject: false, token)),
            rejected => { _ = DispatchAsync(queued, reject: true, CancellationToken.None); }))
        {
            _ = DispatchAsync(queued, reject: true, CancellationToken.None);
        }
    }

    // Claims a queued request and runs it, or rejects it as busy when the dispatcher is saturated.
    async Task DispatchAsync(QueuedInvocation queued, bool reject, CancellationToken ct)
    {
        RpcInboundRequest request;
        lock (_sync)
        {
            if (!_queued.Remove(queued.Id) || queued.Request is not { } claimed)
                return;
            request = claimed;
            queued.Request = null;
            _claimed.Add(queued.Id);
        }

        try
        {
            if (reject)
                await SendBusyAsync(request, queued.ConnectionClosed).ConfigureAwait(false);
            else
                await InvokeAsync(request, queued.ReceivedAt, queued.ConnectionClosed, ct).ConfigureAwait(false);
        }
        finally
        {
            Forget(request.InvocationId);
            await SendCleanupAckAsync(queued.CorrelationId, queued.ConnectionClosed).ConfigureAwait(false);
        }
    }

    async Task InvokeAsync(RpcInboundRequest request, long receivedAt, CancellationToken connectionClosed, CancellationToken ct)
    {
        try
        {
            if (connectionClosed.IsCancellationRequested)
                return;

            var writer = new RpcResponseWriter(_send, request.CorrelationId, connectionClosed);
            await using var writerScope = writer.ConfigureAwait(false);
            var remaining = RemainingBudget(request, receivedAt);
            using var budget = remaining is { } initialBudget ? new CancellationTokenSource(initialBudget, _time) : null;
            using var invocation = CancellationTokenSource.CreateLinkedTokenSource(
                ct,
                connectionClosed,
                budget?.Token ?? CancellationToken.None);

            bool cancelledBeforeStart;
            lock (_sync)
            {
                _claimed.Remove(request.InvocationId);
                cancelledBeforeStart = _cancelledBeforeStart.Remove(request.InvocationId);
                _active[request.InvocationId] = invocation;
            }

            LocalWorker? reserved = null;
            try
            {
                if (cancelledBeforeStart || remaining == TimeSpan.Zero)
                    await invocation.CancelAsync().ConfigureAwait(false);

                try
                {
                    reserved = await ReserveAsync(request.Route, invocation.Token).ConfigureAwait(false);
                    if (reserved is null)
                    {
                        await SendBusyAsync(request, connectionClosed).ConfigureAwait(false);
                        return;
                    }

                    await reserved.Handler(
                        new RpcRequest(request.Route, request.Body, RemainingBudget(request, receivedAt), _time),
                        writer,
                        invocation.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (invocation.IsCancellationRequested)
                {
                }
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
                if (!writer.IsEnded)
                {
                    try
                    {
                        await writer.SendAsync(
                            RpcWire.EncodeTerminalError(FitzErrorCodes.RpcBackendError, "Local RPC worker failed"),
                            isEnd: true,
                            ct).ConfigureAwait(false);
                    }
                    catch (Exception sendError) when (ExceptionPolicy.IsRecoverable(sendError))
                    {
                        // The handler's failure is the one to report; the lost error frame only
                        // means the caller times out instead of seeing RPC_BACKEND_ERROR.
                        throw new AggregateException(error, sendError);
                    }
                }

                throw;
            }
            finally
            {
                lock (_sync)
                {
                    // Waiting invocations are cancellation targets too; always remove them,
                    // including failures before a worker could be reserved.
                    _active.Remove(request.InvocationId);
                    if (reserved is not null)
                    {
                        reserved.Gate.Release();
                        NotifyAvailabilityLocked();
                    }
                }
            }
        }
        catch (Exception exception) when (ExceptionPolicy.IsRecoverable(exception))
        {
            ReportError(exception);
        }
    }

    // The caller's budget less the time since the request arrived, floored at zero.
    TimeSpan? RemainingBudget(RpcInboundRequest request, long receivedAt)
    {
        if (request.Budget is not { } budget)
            return null;
        var remaining = budget - _time.GetElapsedTime(receivedAt);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    // Takes one unit of a matching worker's concurrency, waiting while every matching worker is
    // busy. Returns null when no worker matches or the bounded wait queue is full.
    async ValueTask<LocalWorker?> ReserveAsync(string route, CancellationToken ct)
    {
        var waiting = false;
        try
        {
            while (true)
            {
                Task available;
                lock (_sync)
                {
                    if (FindWorkerLocked(route, requireCapacity: true) is { } free && free.Gate.Wait(0, CancellationToken.None))
                        return free;
                    if (FindWorkerLocked(route, requireCapacity: false) is null)
                        return null;
                    if (!waiting)
                    {
                        // The broker limits admission independently for each overlapping
                        // registration. Local credit can therefore be released in a different
                        // order; retain a bounded wait rather than spuriously reject that work.
                        var capacity = _workers.Values.Sum(static worker => checked((int)worker.MaxConcurrency));
                        if (_waitingCount >= capacity)
                            return null;
                        _waitingCount++;
                        waiting = true;
                    }

                    available = _availability.Task;
                }

                await available.WaitAsync(ct).ConfigureAwait(false);
                ct.ThrowIfCancellationRequested();
            }
        }
        finally
        {
            if (waiting)
            {
                lock (_sync)
                    _waitingCount--;
            }
        }
    }

    // An exact registration wins; otherwise the most specific matching pattern, ties broken ordinally.
    LocalWorker? FindWorkerLocked(string route, bool requireCapacity)
    {
        if (_workers.TryGetValue(route, out var exact) && (!requireCapacity || exact.Gate.CurrentCount > 0))
            return exact;

        string? bestPattern = null;
        LocalWorker? best = null;
        (int LiteralSegments, int SingleWildcards, int SegmentCount) bestSpecificity = default;
        foreach (var (pattern, worker) in _workers)
        {
            if ((requireCapacity && worker.Gate.CurrentCount == 0) || !RouteValidation.MatchesPattern(route, pattern))
                continue;
            var specificity = GetPatternSpecificity(pattern);
            if (bestPattern is null || specificity.CompareTo(bestSpecificity) > 0 ||
                (specificity == bestSpecificity && string.CompareOrdinal(pattern, bestPattern) < 0))
            {
                bestPattern = pattern;
                best = worker;
                bestSpecificity = specificity;
            }
        }

        return best;
    }

    static (int LiteralSegments, int SingleWildcards, int SegmentCount) GetPatternSpecificity(string pattern)
    {
        var pathStart = pattern.IndexOf("://", StringComparison.Ordinal) + 3;
        var segments = pattern[pathStart..].Split('/');
        var literals = 0;
        var singleWildcards = 0;
        foreach (var segment in segments)
        {
            if (segment == "*")
                singleWildcards++;
            else if (segment != "**")
                literals++;
        }

        return (literals, singleWildcards, segments.Length);
    }

    void NotifyAvailabilityLocked()
    {
        var previous = _availability;
        _availability = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        previous.TrySetResult();
    }

    void Forget(Guid invocationId)
    {
        lock (_sync)
        {
            _queued.Remove(invocationId);
            _claimed.Remove(invocationId);
            _cancelledBeforeStart.Remove(invocationId);
        }
    }

    void RemoveLocked(string pattern)
    {
        _workers.Remove(pattern);
        NotifyAvailabilityLocked();
    }

    async Task UnregisterAsync(string pattern, CancellationToken ct)
    {
        await UnsubscribeWireAsync(pattern, ct).ConfigureAwait(false);
        lock (_sync)
        {
            RemoveLocked(pattern);
            if (_workers.Count == 0)
            {
                _reconnectRegistration?.Dispose();
                _reconnectRegistration = null;
            }
        }
    }

    async ValueTask ResubscribeAsync(CancellationToken ct)
    {
        (string Pattern, uint MaxConcurrency)[] snapshot;
        lock (_sync)
            snapshot = [.. _workers.Select(static entry => (entry.Key, entry.Value.MaxConcurrency))];

        var restored = new List<string>(snapshot.Length);
        try
        {
            foreach (var (pattern, maxConcurrency) in snapshot)
            {
                await SubscribeWireAsync(pattern, maxConcurrency, ct).ConfigureAwait(false);
                restored.Add(pattern);
            }
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            foreach (var pattern in restored)
            {
                try
                {
                    await UnsubscribeWireAsync(pattern, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception cleanupError) when (ExceptionPolicy.IsRecoverable(cleanupError))
                {
                    // Best effort; preserve the original restore failure.
                }
            }

            throw;
        }
    }

    async Task SubscribeWireAsync(string pattern, uint maxConcurrency, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        writer.WriteU32(maxConcurrency);
        writer.WriteU8(1);
        var response = await _request(MessageTypes.RpcSubscribeWorker, writer.WrittenMemory, ct).ConfigureAwait(false);
        _ = RpcWire.ReadSuccess(response, "REGISTER");
    }

    async Task UnsubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.RpcUnsubscribeWorker, writer.WrittenMemory, ct).ConfigureAwait(false);
        _ = RpcWire.ReadSuccess(response, "UNREGISTER");
    }

    async Task SendBusyAsync(RpcInboundRequest request, CancellationToken connectionClosed)
    {
        try
        {
            var writer = new RpcResponseWriter(_send, request.CorrelationId, connectionClosed);
            await using var writerScope = writer.ConfigureAwait(false);
            await writer.SendAsync(
                RpcWire.EncodeTerminalError(FitzErrorCodes.RpcBackpressure, "Local RPC worker is overloaded", 5, 0),
                isEnd: true,
                connectionClosed).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
        }
    }

    async Task SendCleanupAckAsync(byte[] correlationId, CancellationToken connectionClosed)
    {
        if (connectionClosed.IsCancellationRequested)
            return;

        using var ack = new BinaryBufferWriter();
        ack.WriteU8(3);
        ack.WriteBytes(correlationId);
        try
        {
            await _send(MessageTypes.RpcCancel, ack.WrittenMemory, connectionClosed).ConfigureAwait(false);
        }
        catch (Exception exception) when (ExceptionPolicy.IsRecoverable(exception))
        {
            ReportError(exception);
        }
    }

    async Task CancelAsync(CancellationTokenSource cancellation)
    {
        try
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception exception) when (ExceptionPolicy.IsRecoverable(exception))
        {
            ReportError(exception);
        }
    }

    // Diagnostic sinks must not tear down notification dispatch.
    void ReportError(Exception exception)
    {
        try
        {
            _onWorkerError?.Invoke(exception);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
        }
    }

    /// <summary>A registered worker and its concurrency limit.</summary>
    sealed class LocalWorker(Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> handler, uint maxConcurrency)
    {
        internal Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> Handler { get; } = handler;

        internal uint MaxConcurrency { get; } = maxConcurrency;

        internal SemaphoreSlim Gate { get; } = new(checked((int)maxConcurrency), checked((int)maxConcurrency));
    }

    /// <summary>A received request waiting for the dispatcher; <see cref="Request"/> is cleared once claimed or discarded.</summary>
    sealed class QueuedInvocation(RpcInboundRequest request, long receivedAt, CancellationToken connectionClosed)
    {
        internal Guid Id { get; } = request.InvocationId;

        internal byte[] CorrelationId { get; } = request.CorrelationId;

        internal RpcInboundRequest? Request { get; set; } = request;

        internal long ReceivedAt { get; } = receivedAt;

        internal CancellationToken ConnectionClosed { get; } = connectionClosed;
    }
}
