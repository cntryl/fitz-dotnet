using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Connection;

/// <summary>
/// Routes framed responses and notifications to pending requests and handlers.
/// </summary>
public sealed class Multiplexer : IDisposable
{
    readonly object _gate = new();
    readonly Dictionary<ushort, LinkedList<PendingRequest>> _pending = [];
    readonly Dictionary<ushort, SemaphoreSlim> _requestLanes = [];
    readonly Dictionary<ushort, Dictionary<long, NotificationHandler>> _notificationHandlers = [];
    readonly Queue<NotificationDispatch> _restoringNotifications = [];
    readonly Action<Exception>? _onDispatchError;
    readonly Action<Exception>? _onSessionDesynchronized;
    readonly Channel<NotificationDispatch> _notificationQueue;
    readonly Task _notificationPump;
    ConnectionState _state = ConnectionState.Disconnected;
    CancellationTokenSource _laneStateCts = new();
    long _sessionEpoch;
    long _nextHandlerId;
    Exception? _notificationRestoreFailure;
    bool _notificationRestoreActive;
    bool _disposed;

    public Multiplexer(
        Action<Exception>? onDispatchError = null,
        Action<Exception>? onSessionDesynchronized = null)
    {
        _onDispatchError = onDispatchError;
        _onSessionDesynchronized = onSessionDesynchronized;
        _notificationQueue = Channel.CreateBounded<NotificationDispatch>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });
        _notificationPump = Task.Run(PumpNotificationsAsync);
        _ = _notificationPump.ContinueWith(
            static task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Resets request-lane state for a new transport session before auth completes.
    /// </summary>
    public void BeginSession()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_laneStateCts.IsCancellationRequested)
            {
                return;
            }

            _laneStateCts = new CancellationTokenSource();
            _sessionEpoch++;
        }
    }

    /// <summary>
    /// Marks the connection as authenticated and ready to dispatch frames.
    /// </summary>
    public void SetConnected()
    {
        BeginSession();
        lock (_gate)
        {
            _state = ConnectionState.Authenticated;
        }
    }

    /// <summary>
    /// Buffers notifications until reconnect listeners have atomically installed their new-session state.
    /// </summary>
    internal void BeginNotificationRestore()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _restoringNotifications.Clear();
            _notificationRestoreFailure = null;
            _notificationRestoreActive = true;
        }
    }

    /// <summary>
    /// Publishes notifications received while reconnect listeners were restoring new-session state.
    /// </summary>
    internal void CompleteNotificationRestore()
    {
        Exception? failure;
        lock (_gate)
        {
            if (!_notificationRestoreActive)
            {
                return;
            }

            failure = _notificationRestoreFailure;
            if (failure is null)
            {
                while (_restoringNotifications.Count > 0)
                {
                    if (!_notificationQueue.Writer.TryWrite(_restoringNotifications.Dequeue()))
                    {
                        failure = new SubscriptionBackpressureException(
                            "The multiplexer notification dispatch queue filled while reconnect state was being restored.");
                        break;
                    }
                }
            }

            _restoringNotifications.Clear();
            _notificationRestoreFailure = null;
            _notificationRestoreActive = false;
        }

        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    /// Discards notifications owned by an unsuccessful reconnect attempt.
    /// </summary>
    internal void AbortNotificationRestore()
    {
        lock (_gate)
        {
            _restoringNotifications.Clear();
            _notificationRestoreFailure = null;
            _notificationRestoreActive = false;
        }
    }

    /// <summary>
    /// Marks the connection as disconnected and cancels all pending requests.
    /// </summary>
    public void SetDisconnected()
    {
        CancellationTokenSource? laneStateCts = null;
        lock (_gate)
        {
            _state = ConnectionState.Disconnected;
            _restoringNotifications.Clear();
            _notificationRestoreFailure = null;
            _notificationRestoreActive = false;
            if (!_laneStateCts.IsCancellationRequested)
            {
                laneStateCts = _laneStateCts;
            }
        }

        laneStateCts?.Cancel();
        CancelAll();
    }

    /// <summary>
    /// Registers an owned notification handler for a message type.
    /// </summary>
    public IDisposable RegisterNotificationHandler(ushort messageType, Action<byte[]> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return RegisterNotificationHandlerCore(messageType, NotificationHandler.FromOwned(handler));
    }

    /// <summary>
    /// Registers a borrowed notification handler for a message type.
    /// </summary>
    internal IDisposable RegisterBorrowedNotificationHandler(ushort messageType, Action<ReadOnlyMemory<byte>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        return RegisterNotificationHandlerCore(messageType, NotificationHandler.FromBorrowed(handler));
    }

    NotificationRegistration RegisterNotificationHandlerCore(ushort messageType, NotificationHandler handler)
    {
        long handlerId;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            handlerId = ++_nextHandlerId;
            if (!_notificationHandlers.TryGetValue(messageType, out var registrations))
            {
                registrations = [];
                _notificationHandlers[messageType] = registrations;
            }

            registrations[handlerId] = handler;
        }

        return new NotificationRegistration(this, messageType, handlerId);
    }

    /// <summary>
    /// Dispatches a frame payload to a pending request or notification handlers.
    /// </summary>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Notification handlers are user callbacks and must not break frame dispatch.")]
    public void Dispatch(ushort messageType, ReadOnlyMemory<byte> payload)
    {
        PendingRequest? pending = null;
        NotificationHandler[]? handlers = null;

        lock (_gate)
        {
            if (_state != ConnectionState.Authenticated)
            {
                return;
            }

            if (_pending.TryGetValue(messageType, out var pendingQueue) && pendingQueue.Count > 0)
            {
                pending = FindPendingRequestByPayload(pendingQueue, payload);
                if (pending is null)
                {
                    if (_notificationHandlers.TryGetValue(messageType, out var registeredHandlers) && registeredHandlers.Count > 0)
                    {
                        handlers = new NotificationHandler[registeredHandlers.Count];
                        registeredHandlers.Values.CopyTo(handlers, 0);
                    }
                }
                else
                {
                    pendingQueue.Remove(pending.QueueNode!);
                    if (pendingQueue.Count == 0)
                    {
                        _pending.Remove(messageType);
                    }
                }
            }

            if (pending is null && handlers is null && _notificationHandlers.TryGetValue(messageType, out var registeredHandlersNoPending) && registeredHandlersNoPending.Count > 0)
            {
                handlers = new NotificationHandler[registeredHandlersNoPending.Count];
                registeredHandlersNoPending.Values.CopyTo(handlers, 0);
            }

            if (pending is null && handlers is null)
            {
                return;
            }

            if (pending is null && handlers is not null && _notificationRestoreActive)
            {
                var restoringPayload = payload.IsEmpty ? Array.Empty<byte>() : payload.ToArray();
                foreach (var handler in handlers)
                {
                    if (_restoringNotifications.Count >= 1024)
                    {
                        _notificationRestoreFailure ??= new SubscriptionBackpressureException(
                            "The reconnect notification buffer is full.");
                        break;
                    }

                    _restoringNotifications.Enqueue(new NotificationDispatch(handler, restoringPayload));
                }

                return;
            }
        }

        if (pending is not null)
        {
            pending.CompleteResponse(payload.IsEmpty ? Array.Empty<byte>() : payload.ToArray());
            return;
        }

        if (handlers is null)
        {
            return;
        }

        var ownedPayload = payload.IsEmpty ? Array.Empty<byte>() : payload.ToArray();
        foreach (var handler in handlers)
        {
            if (!_notificationQueue.Writer.TryWrite(new NotificationDispatch(handler, ownedPayload)))
            {
                ReportDispatchError(new SubscriptionBackpressureException(
                    "The multiplexer notification dispatch queue is full."));
            }
        }
    }

    /// <summary>
    /// Dispatches a buffered payload to a pending request or notification handlers.
    /// </summary>
    public void Dispatch(ushort messageType, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        Dispatch(messageType, payload.AsMemory());
    }

    /// <summary>
    /// Sends a request frame and awaits the matching response.
    /// </summary>
    public async Task<byte[]> RequestAsync(
        ushort messageType,
        byte[] frameData,
        Func<ReadOnlyMemory<byte>, CancellationToken, Task> send,
        TimeSpan timeout,
        Func<ReadOnlyMemory<byte>, bool>? responseMatcher = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(send);

        SemaphoreSlim? lane = null;
        CancellationTokenSource? laneWaitCts = null;
        if (responseMatcher is null)
        {
            lane = GetLane(messageType);
            laneWaitCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, GetLaneWaitToken());
            try
            {
                await lane.WaitAsync(laneWaitCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new ConnectionException("Connection closed or reset");
            }
        }

        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new PendingRequest(this, messageType, timeout, tcs, responseMatcher, cancellationToken);

        try
        {
            lock (_gate)
            {
                if (_disposed || _laneStateCts.IsCancellationRequested)
                {
                    throw new ConnectionException("Connection closed or reset");
                }

                if (!_pending.TryGetValue(messageType, out var pendingQueue))
                {
                    pendingQueue = new LinkedList<PendingRequest>();
                    _pending[messageType] = pendingQueue;
                }

                request.QueueNode = pendingQueue.AddLast(request);
                request.SessionEpoch = _sessionEpoch;
            }

            using var timeoutCts = timeout == Timeout.InfiniteTimeSpan ? null : new CancellationTokenSource(timeout);
            using var cancellationRegistration = cancellationToken.CanBeCanceled
                ? cancellationToken.Register(static state => ((PendingRequest)state!).Cancel(), request)
                : default;
            using var timeoutRegistration = timeoutCts?.Token.Register(static state => ((PendingRequest)state!).Timeout(), request) ?? default;

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                await send(frameData, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                request.FailSend();
                throw;
            }

            request.MarkSent();
            return await tcs.Task.ConfigureAwait(false);
        }
        finally
        {
            laneWaitCts?.Dispose();
            lane?.Release();
        }
    }

    /// <summary>
    /// Fails all pending requests with a connection-closed error.
    /// </summary>
    public void CancelAll()
    {
        List<PendingRequest> pending;
        lock (_gate)
        {
            pending = [];
            foreach (var queue in _pending.Values)
            {
                for (var node = queue.First; node is not null; node = node.Next)
                {
                    pending.Add(node.Value);
                }
            }

            _pending.Clear();
        }

        foreach (var request in pending)
        {
            request.FailConnection(new ConnectionException("Connection closed or reset"));
        }
    }

    public void Dispose()
    {
        SetDisconnected();
        _notificationQueue.Writer.TryComplete();

        lock (_gate)
        {
            _disposed = true;
            _laneStateCts.Dispose();
            _requestLanes.Clear();
            _notificationHandlers.Clear();
        }
    }

    internal async Task CompleteNotificationDispatchAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        _notificationQueue.Writer.TryComplete();
        await _notificationPump.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Notification callbacks are isolated from the receive loop and reported through the configured diagnostic sink.")]
    async Task PumpNotificationsAsync()
    {
        await foreach (var notification in _notificationQueue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                notification.Handler.Invoke(notification.Payload);
            }
            catch (Exception exception)
            {
                ReportDispatchError(exception);
            }
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "A diagnostic callback must not break notification dispatch.")]
    void ReportDispatchError(Exception exception)
    {
        try
        {
            _onDispatchError?.Invoke(exception);
        }
        catch
        {
        }
    }

    void RemoveRequest(PendingRequest request)
    {
        lock (_gate)
        {
            if (_pending.TryGetValue(request.MessageType, out var queue))
            {
                if (request.QueueNode is not null)
                {
                    if (request.QueueNode.List == queue)
                    {
                        queue.Remove(request.QueueNode);
                    }

                    request.QueueNode = null;
                }

                if (queue.Count == 0)
                {
                    _pending.Remove(request.MessageType);
                }
            }

            request.QueueNode = null;
        }
    }

    SemaphoreSlim GetLane(ushort messageType)
    {
        lock (_gate)
        {
            if (_disposed)
            {
                throw new ConnectionException("Connection multiplexer is closed");
            }

            if (_requestLanes.TryGetValue(messageType, out var lane))
            {
                return lane;
            }

            lane = new SemaphoreSlim(1, 1);
            _requestLanes[messageType] = lane;
            return lane;
        }
    }

    CancellationToken GetLaneWaitToken()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                throw new ConnectionException("Connection multiplexer is closed");
            }

            return _laneStateCts.Token;
        }
    }

    void Desynchronize(PendingRequest abandonedRequest, ConnectionException failure)
    {
        List<PendingRequest> affected;
        CancellationTokenSource? laneStateCts = null;
        var transitioned = false;
        lock (_gate)
        {
            RemoveRequest(abandonedRequest);
            if (abandonedRequest.SessionEpoch != _sessionEpoch || _state != ConnectionState.Authenticated)
            {
                return;
            }

            _state = ConnectionState.Disconnected;
            transitioned = true;
            if (!_laneStateCts.IsCancellationRequested)
            {
                laneStateCts = _laneStateCts;
            }

            affected = [];
            foreach (var queue in _pending.Values)
            {
                for (var node = queue.First; node is not null; node = node.Next)
                {
                    affected.Add(node.Value);
                }
            }

            _pending.Clear();
        }

        laneStateCts?.Cancel();
        foreach (var request in affected)
        {
            request.FailConnection(new ConnectionException(
                $"Connection reset after response lane {abandonedRequest.MessageType} became desynchronized."));
        }

        if (transitioned)
        {
            ReportSessionDesynchronized(failure);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "A session-reset callback must not break request completion.")]
    void ReportSessionDesynchronized(Exception exception)
    {
        try
        {
            _onSessionDesynchronized?.Invoke(exception);
        }
        catch
        {
        }
    }

    void RemoveNotificationHandler(ushort messageType, long handlerId)
    {
        lock (_gate)
        {
            if (!_notificationHandlers.TryGetValue(messageType, out var handlers))
            {
                return;
            }

            handlers.Remove(handlerId);
            if (handlers.Count == 0)
            {
                _notificationHandlers.Remove(messageType);
            }
        }
    }

    sealed class PendingRequest
    {
        enum CompletionKind
        {
            Active,
            Response,
            Canceled,
            TimedOut,
            SendFailed,
            ConnectionFailed,
        }

        readonly object _stateGate = new();
        readonly Multiplexer _owner;
        readonly ushort _messageType;
        readonly TimeSpan _timeout;
        readonly CancellationToken _cancellationToken;
        readonly Func<ReadOnlyMemory<byte>, bool>? _responseMatcher;
        CompletionKind _completionKind;
        bool _sent;

        internal PendingRequest(
            Multiplexer owner,
            ushort messageType,
            TimeSpan timeout,
            TaskCompletionSource<byte[]> promise,
            Func<ReadOnlyMemory<byte>, bool>? responseMatcher,
            CancellationToken cancellationToken)
        {
            _owner = owner;
            _messageType = messageType;
            _timeout = timeout;
            _cancellationToken = cancellationToken;
            _responseMatcher = responseMatcher;
            Promise = promise;
        }

        internal LinkedListNode<PendingRequest>? QueueNode { get; set; }

        internal long SessionEpoch { get; set; }

        internal ushort MessageType => _messageType;

        internal TimeSpan TimeoutDuration => _timeout;

        internal TaskCompletionSource<byte[]> Promise { get; }

        [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "A user-supplied response matcher must not break frame dispatch.")]
        internal bool MatchesResponse(ReadOnlyMemory<byte> payload)
        {
            if (_responseMatcher is null)
            {
                return false;
            }

            try
            {
                return _responseMatcher(payload);
            }
            catch
            {
                return false;
            }
        }

        internal bool IsCorrelated => _responseMatcher is not null;

        internal void Cancel()
        {
            bool desynchronize;
            lock (_stateGate)
            {
                if (_completionKind != CompletionKind.Active)
                {
                    return;
                }

                _completionKind = CompletionKind.Canceled;
                desynchronize = _sent && !IsCorrelated;
            }

            Promise.TrySetCanceled(_cancellationToken);
            if (desynchronize)
            {
                _owner.Desynchronize(this, new ConnectionException(
                    $"Response lane {_messageType} was reset after a sent request was canceled."));
            }
            else
            {
                _owner.RemoveRequest(this);
            }
        }

        internal void Timeout()
        {
            bool desynchronize;
            lock (_stateGate)
            {
                if (_completionKind != CompletionKind.Active)
                {
                    return;
                }

                _completionKind = CompletionKind.TimedOut;
                desynchronize = _sent && !IsCorrelated;
            }

            var suffix = desynchronize ? "; the response lane was reset to prevent response misdelivery." : string.Empty;
            Promise.TrySetException(new RequestTimeoutException(
                $"Request timeout for message type {_messageType} after {_timeout.TotalMilliseconds}ms{suffix}"));
            if (desynchronize)
            {
                _owner.Desynchronize(this, new ConnectionException(
                    $"Response lane {_messageType} timed out and was reset to prevent response misdelivery."));
            }
            else
            {
                _owner.RemoveRequest(this);
            }
        }

        internal void MarkSent()
        {
            CompletionKind completionKind;
            lock (_stateGate)
            {
                _sent = true;
                completionKind = _completionKind;
            }

            if (IsCorrelated || completionKind is not (CompletionKind.Canceled or CompletionKind.TimedOut))
            {
                return;
            }

            var reason = completionKind == CompletionKind.Canceled
                ? $"Response lane {_messageType} was reset after a sent request was canceled."
                : $"Response lane {_messageType} timed out and was reset to prevent response misdelivery.";
            _owner.Desynchronize(this, new ConnectionException(reason));
        }

        internal void CompleteResponse(byte[] payload)
        {
            lock (_stateGate)
            {
                if (_completionKind != CompletionKind.Active)
                {
                    return;
                }

                _completionKind = CompletionKind.Response;
            }

            Promise.TrySetResult(payload);
        }

        internal void FailSend()
        {
            lock (_stateGate)
            {
                if (_completionKind == CompletionKind.Active)
                {
                    _completionKind = CompletionKind.SendFailed;
                }
            }

            _owner.RemoveRequest(this);
        }

        internal void FailConnection(ConnectionException failure)
        {
            lock (_stateGate)
            {
                if (_completionKind != CompletionKind.Active)
                {
                    return;
                }

                _completionKind = CompletionKind.ConnectionFailed;
            }

            Promise.TrySetException(failure);
        }
    }

    static PendingRequest? FindPendingRequestByPayload(
        LinkedList<PendingRequest> pendingQueue,
        ReadOnlyMemory<byte> payload)
    {
        if (pendingQueue.Count <= 0)
        {
            return null;
        }

        var hasCorrelatedRequest = false;
        for (var node = pendingQueue.First; node is not null; node = node.Next)
        {
            if (node.Value.IsCorrelated)
            {
                hasCorrelatedRequest = true;
                break;
            }
        }

        if (!hasCorrelatedRequest)
        {
            return pendingQueue.First!.Value;
        }

        for (var candidateNode = pendingQueue.First; candidateNode is not null; candidateNode = candidateNode.Next)
        {
            var candidate = candidateNode.Value;
            if (!candidate.IsCorrelated)
            {
                continue;
            }

            if (candidate.MatchesResponse(payload))
            {
                return candidate;
            }
        }

        return null;
    }

    readonly struct NotificationHandler
    {
        readonly Action<byte[]>? _ownedHandler;
        readonly Action<ReadOnlyMemory<byte>>? _borrowedHandler;

        NotificationHandler(Action<byte[]>? ownedHandler, Action<ReadOnlyMemory<byte>>? borrowedHandler)
        {
            _ownedHandler = ownedHandler;
            _borrowedHandler = borrowedHandler;
        }

        internal static NotificationHandler FromOwned(Action<byte[]> handler) => new(handler, null);

        internal static NotificationHandler FromBorrowed(Action<ReadOnlyMemory<byte>> handler) => new(null, handler);

        internal void Invoke(byte[] payload)
        {
            if (_borrowedHandler is not null)
            {
                _borrowedHandler(payload);
                return;
            }

            _ownedHandler!(payload);
        }
    }

    readonly record struct NotificationDispatch(NotificationHandler Handler, byte[] Payload);

    sealed class NotificationRegistration : IDisposable
    {
        readonly Multiplexer _owner;
        readonly ushort _messageType;
        readonly long _handlerId;
        int _disposed;

        internal NotificationRegistration(Multiplexer owner, ushort messageType, long handlerId)
        {
            _owner = owner;
            _messageType = messageType;
            _handlerId = handlerId;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _owner.RemoveNotificationHandler(_messageType, _handlerId);
        }
    }
}
