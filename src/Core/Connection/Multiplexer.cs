using Cntryl.Fitz;
using Cntryl.Fitz.Errors;
using System.Diagnostics.CodeAnalysis;

namespace Cntryl.Fitz.Connection;

/// <summary>
/// Routes framed responses and notifications to pending requests and handlers.
/// </summary>
public sealed class Multiplexer : IDisposable
{
    private readonly object _gate = new();
    private readonly Dictionary<ushort, LinkedList<PendingRequest>> _pending = new();
    private readonly Dictionary<ushort, SemaphoreSlim> _requestLanes = new();
    private readonly Dictionary<ushort, Dictionary<long, NotificationHandler>> _notificationHandlers = new();
    private readonly Dictionary<ushort, int> _optionalResponses = new();
    private ConnectionState _state = ConnectionState.Disconnected;
    private CancellationTokenSource _laneStateCts = new();
    private long _nextHandlerId;

    /// <summary>
    /// Resets request-lane state for a new transport session before auth completes.
    /// </summary>
    public void BeginSession()
    {
        lock (_gate)
        {
            if (!_laneStateCts.IsCancellationRequested)
            {
                return;
            }

            _laneStateCts.Dispose();
            _laneStateCts = new CancellationTokenSource();
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
    /// Marks the connection as disconnected and cancels all pending requests.
    /// </summary>
    public void SetDisconnected()
    {
        CancellationTokenSource? laneStateCts = null;
        lock (_gate)
        {
            _state = ConnectionState.Disconnected;
            _optionalResponses.Clear();
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

    private NotificationRegistration RegisterNotificationHandlerCore(ushort messageType, NotificationHandler handler)
    {
        long handlerId;
        lock (_gate)
        {
            handlerId = ++_nextHandlerId;
            if (!_notificationHandlers.TryGetValue(messageType, out var registrations))
            {
                registrations = new Dictionary<long, NotificationHandler>();
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
        var ignoredOptionalResponse = false;

        lock (_gate)
        {
            if (_pending.TryGetValue(messageType, out var pendingQueue) && pendingQueue.Count > 0)
            {
                pending = FindPendingRequestByPayload(pendingQueue, payload);
                if (ShouldConsumeOptionalResponse(messageType, pending))
                {
                    ignoredOptionalResponse = true;
                }
                else if (pending is null)
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
                var optional = _optionalResponses.GetValueOrDefault(messageType);
                if (optional > 0)
                {
                    if (optional == 1)
                    {
                        _optionalResponses.Remove(messageType);
                    }
                    else
                    {
                        _optionalResponses[messageType] = optional - 1;
                    }

                    return;
                }

                if (_state != ConnectionState.Authenticated)
                {
                    return;
                }
            }
        }

        if (ignoredOptionalResponse)
        {
            return;
        }

        if (pending is not null)
        {
            pending.Promise.TrySetResult(payload.IsEmpty ? Array.Empty<byte>() : payload.ToArray());
            return;
        }

        if (handlers is null)
        {
            return;
        }

        byte[]? ownedPayload = null;
        foreach (var handler in handlers)
        {
            try
            {
                handler.Invoke(payload, ref ownedPayload);
            }
            catch
            {
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
    /// Marks the next response for a message type as optional.
    /// </summary>
    public Action ExpectOptionalResponse(ushort messageType)
    {
        lock (_gate)
        {
            _optionalResponses[messageType] = _optionalResponses.GetValueOrDefault(messageType) + 1;
        }

        return () =>
        {
            lock (_gate)
            {
                var current = _optionalResponses.GetValueOrDefault(messageType);
                if (current <= 1)
                {
                    _optionalResponses.Remove(messageType);
                }
                else
                {
                    _optionalResponses[messageType] = current - 1;
                }
            }
        };
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

        using var timeoutCts = timeout == Timeout.InfiniteTimeSpan ? null : new CancellationTokenSource(timeout);
        using var cancellationRegistration = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => ((PendingRequest)state!).Cancel(), request)
            : default;
        using var timeoutRegistration = timeoutCts?.Token.Register(static state => ((PendingRequest)state!).Timeout(), request) ?? default;

        try
        {
            lock (_gate)
            {
                if (_laneStateCts.IsCancellationRequested)
                {
                    throw new ConnectionException("Connection closed or reset");
                }

                if (!_pending.TryGetValue(messageType, out var pendingQueue))
                {
                    pendingQueue = new LinkedList<PendingRequest>();
                    _pending[messageType] = pendingQueue;
                }

                request.QueueNode = pendingQueue.AddLast(request);
            }

            await send(frameData, cancellationToken).ConfigureAwait(false);
            return await tcs.Task.ConfigureAwait(false);
        }
        catch
        {
            request.MarkCanceledAndFail(new ConnectionException("Request was canceled before sending a frame"));
            throw;
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
        PendingRequest[] pending;
        lock (_gate)
        {
            pending = _pending
                .SelectMany(pair => pair.Value)
                .ToArray();
            _pending.Clear();
        }

        foreach (var request in pending)
        {
            request.Promise.TrySetException(new ConnectionException("Connection closed or reset"));
        }
    }

    public void Dispose()
    {
        SetDisconnected();

        lock (_gate)
        {
            _laneStateCts.Dispose();
            foreach (var lane in _requestLanes.Values)
            {
                lane.Dispose();
            }

            _requestLanes.Clear();
            _notificationHandlers.Clear();
        }
    }

    private void RemoveRequest(PendingRequest request)
    {
        lock (_gate)
        {
            if (request.MessageType == 0)
            {
                return;
            }

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

    private SemaphoreSlim GetLane(ushort messageType)
    {
        lock (_gate)
        {
            if (_requestLanes.TryGetValue(messageType, out var lane))
            {
                return lane;
            }

            lane = new SemaphoreSlim(1, 1);
            _requestLanes[messageType] = lane;
            return lane;
        }
    }

    private CancellationToken GetLaneWaitToken()
    {
        lock (_gate)
        {
            return _laneStateCts.Token;
        }
    }

    private bool ShouldConsumeOptionalResponse(ushort messageType, PendingRequest? pending)
    {
        if (pending is not null && pending.IsCorrelated)
        {
            return false;
        }

        var optional = _optionalResponses.GetValueOrDefault(messageType);
        if (optional <= 0)
        {
            return false;
        }

        if (optional == 1)
        {
            _optionalResponses.Remove(messageType);
        }
        else
        {
            _optionalResponses[messageType] = optional - 1;
        }

        return true;
    }

    private void MarkOptionalResponse(ushort messageType)
    {
        lock (_gate)
        {
            _optionalResponses[messageType] = _optionalResponses.GetValueOrDefault(messageType) + 1;
        }
    }

    private void RemovePending(ushort messageType, PendingRequest request)
    {
        lock (_gate)
        {
            RemoveRequest(request);
        }
    }

    private void RemoveNotificationHandler(ushort messageType, long handlerId)
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

    private sealed class PendingRequest
    {
        private readonly Multiplexer _owner;
        private readonly ushort _messageType;
        private readonly TimeSpan _timeout;
        private readonly CancellationToken _cancellationToken;
        private readonly Func<ReadOnlyMemory<byte>, bool>? _responseMatcher;

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

        internal ushort MessageType => _messageType;

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
            _owner.RemoveRequest(this);
            Promise.TrySetCanceled(_cancellationToken);
        }

        internal void Timeout()
        {
            if (!IsCorrelated)
            {
                _owner.MarkOptionalResponse(_messageType);
            }

            _owner.RemoveRequest(this);
            Promise.TrySetException(new RequestTimeoutException($"Request timeout for message type {_messageType} after {_timeout.TotalMilliseconds}ms"));
        }

        internal void MarkCanceledAndFail(Exception failure)
        {
            _owner.RemoveRequest(this);
            Promise.TrySetException(failure);
        }
    }

    private static PendingRequest? FindPendingRequestByPayload(
        LinkedList<PendingRequest> pendingQueue,
        ReadOnlyMemory<byte> payload)
    {
        if (pendingQueue.Count <= 0)
        {
            return null;
        }

        if (!pendingQueue.Any(pending => pending.IsCorrelated))
        {
            return pendingQueue.First!.Value;
        }

        PendingRequest? firstUncorrelated = null;
        for (var candidateNode = pendingQueue.First; candidateNode is not null; candidateNode = candidateNode.Next)
        {
            var candidate = candidateNode.Value;
            if (!candidate.IsCorrelated)
            {
                firstUncorrelated ??= candidate;
                continue;
            }

            if (candidate.MatchesResponse(payload))
            {
                return candidate;
            }
        }

        return firstUncorrelated;
    }

    private readonly struct NotificationHandler
    {
        private readonly Action<byte[]>? _ownedHandler;
        private readonly Action<ReadOnlyMemory<byte>>? _borrowedHandler;

        private NotificationHandler(Action<byte[]>? ownedHandler, Action<ReadOnlyMemory<byte>>? borrowedHandler)
        {
            _ownedHandler = ownedHandler;
            _borrowedHandler = borrowedHandler;
        }

        internal static NotificationHandler FromOwned(Action<byte[]> handler)
        {
            return new NotificationHandler(handler, null);
        }

        internal static NotificationHandler FromBorrowed(Action<ReadOnlyMemory<byte>> handler)
        {
            return new NotificationHandler(null, handler);
        }

        internal void Invoke(ReadOnlyMemory<byte> payload, ref byte[]? ownedPayload)
        {
            if (_borrowedHandler is not null)
            {
                _borrowedHandler(payload);
                return;
            }

            ownedPayload ??= payload.IsEmpty ? Array.Empty<byte>() : payload.ToArray();
            _ownedHandler!(ownedPayload);
        }
    }

    private sealed class NotificationRegistration : IDisposable
    {
        private readonly Multiplexer _owner;
        private readonly ushort _messageType;
        private readonly long _handlerId;
        private int _disposed;

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
