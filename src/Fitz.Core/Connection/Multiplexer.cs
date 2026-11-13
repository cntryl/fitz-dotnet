using System.Threading.Channels;

namespace Cntryl.Fitz.Connection;

/// <summary>
/// Routes correlated responses to the requests waiting on them and notification frames to the
/// handlers registered for their message type.
/// </summary>
/// <remarks>
/// Every request carries a non-zero correlation identifier that the broker echoes on its
/// response, so any number of requests of the same message type may be in flight at once.
/// Notifications are copied out of the transport buffer once and fanned out on a dedicated
/// pump, so a slow handler never stalls the receive loop.
/// </remarks>
sealed class Multiplexer : IDisposable
{
    /// <summary>Canceled-request identifiers retained before the session is reset instead.</summary>
    internal const int DefaultMaxCorrelationTombstones = 1024;

    /// <summary>Notification frames queued for the dispatch pump before delivery fails.</summary>
    internal const int NotificationQueueCapacity = 1024;

    /// <summary>Notification frames buffered while reconnect state is being restored.</summary>
    internal const int RestoreBufferCapacity = 1024;

    readonly Lock _gate = new();
    readonly Dictionary<ulong, PendingRequest> _correlated = [];
    // A canceled or timed-out request's identifier is remembered until its late response
    // arrives, so that response is recognised and dropped rather than treated as a protocol error.
    readonly HashSet<ulong> _correlationTombstones = [];
    readonly int _maxCorrelationTombstones;
    readonly Dictionary<ushort, Dictionary<long, Action<ReadOnlyMemory<byte>>>> _notificationHandlers = [];
    // Registration is rare and dispatch is per frame, so the fan-out array is rebuilt on
    // registration changes rather than on every notification.
    readonly Dictionary<ushort, Action<ReadOnlyMemory<byte>>[]> _notificationHandlerSnapshots = [];
    readonly Queue<NotificationDispatch> _restoringNotifications = [];
    readonly Action<Exception> _onDispatchError;
    readonly Action<Exception> _onSessionReset;
    readonly Channel<NotificationDispatch> _notificationQueue;
    readonly Task _notificationPump;
    readonly TimeProvider _time;
    bool _connected;
    long _sessionEpoch;
    long _nextHandlerId;
    Exception? _notificationRestoreFailure;
    bool _notificationRestoreActive;
    bool _disposed;

    /// <summary>Creates a multiplexer.</summary>
    /// <param name="onDispatchError">
    /// Receives a notification handler's failure, or a delivery failure. Must not throw.
    /// </param>
    /// <param name="onSessionReset">
    /// Invoked when the multiplexer abandons the session, so the owner can reconnect. Must not throw.
    /// </param>
    /// <param name="maxCorrelationTombstones">Maximum canceled request identifiers retained before resetting the session.</param>
    /// <param name="timeProvider">The clock that times out requests; the system clock by default.</param>
    public Multiplexer(
        Action<Exception>? onDispatchError = null,
        Action<Exception>? onSessionReset = null,
        int maxCorrelationTombstones = DefaultMaxCorrelationTombstones,
        TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        ArgumentOutOfRangeException.ThrowIfNegative(maxCorrelationTombstones);
        _maxCorrelationTombstones = maxCorrelationTombstones;
        _onDispatchError = onDispatchError ?? (static _ => { });
        _onSessionReset = onSessionReset ?? (static _ => { });
        // Writers use TryWrite, so a full queue fails delivery instead of blocking the receive loop.
        _notificationQueue = Channel.CreateBounded<NotificationDispatch>(new BoundedChannelOptions(NotificationQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _notificationPump = Task.Run(PumpNotificationsAsync);
    }

    /// <summary>Marks the session authenticated, so requests and frames are accepted.</summary>
    public void SetConnected()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _connected = true;
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
    /// <exception cref="SubscriptionBackpressureException">
    /// The restore buffer or dispatch queue overflowed, so notifications were lost.
    /// </exception>
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
            while (failure is null && _restoringNotifications.TryDequeue(out var buffered))
            {
                if (!_notificationQueue.Writer.TryWrite(buffered))
                {
                    failure = new SubscriptionBackpressureException(
                        "The multiplexer notification dispatch queue filled while reconnect state was being restored.");
                }
            }

            ClearRestoreUnsafe();
        }

        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>Discards notifications owned by an unsuccessful reconnect attempt.</summary>
    internal void AbortNotificationRestore()
    {
        lock (_gate)
        {
            ClearRestoreUnsafe();
        }
    }

    /// <summary>Marks the session lost and fails every pending request.</summary>
    public void SetDisconnected()
    {
        List<PendingRequest> pending;
        lock (_gate)
        {
            pending = ResetSessionUnsafe();
        }

        FailAll(pending, new ConnectionException("Connection closed or reset"));
    }

    /// <summary>Registers a handler for one notification message type.</summary>
    /// <param name="messageType">Notification message type to receive.</param>
    /// <param name="handler">
    /// Receives each payload. The memory is a copy owned by the handler, so it may be retained.
    /// </param>
    /// <returns>A registration that stops delivery when disposed.</returns>
    public IDisposable RegisterNotificationHandler(ushort messageType, Action<ReadOnlyMemory<byte>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

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
            RebuildHandlerSnapshotUnsafe(messageType, registrations);
        }

        return new NotificationRegistration(this, messageType, handlerId);
    }

    /// <summary>Completes the request waiting on a correlated response frame.</summary>
    /// <param name="correlationId">Identifier echoed from the request header.</param>
    /// <param name="payload">Response payload; copied before the request observes it.</param>
    /// <param name="responseFinal">
    /// Whether this is the terminal frame for the identifier. Earlier frames are consumed
    /// without completing the request.
    /// </param>
    /// <exception cref="ProtocolException">
    /// No request is waiting on the identifier and it was never canceled locally, so the broker
    /// answered something this session did not ask.
    /// </exception>
    public void DispatchCorrelated(ulong correlationId, ReadOnlyMemory<byte> payload, bool responseFinal = true)
    {
        PendingRequest? pending;
        lock (_gate)
        {
            if (!_connected)
            {
                throw new ProtocolException($"Unexpected response correlation {correlationId}.");
            }

            if (!_correlated.TryGetValue(correlationId, out pending))
            {
                if (!_correlationTombstones.Contains(correlationId))
                {
                    throw new ProtocolException($"Unexpected response correlation {correlationId}.");
                }

                if (responseFinal)
                {
                    _correlationTombstones.Remove(correlationId);
                }

                return;
            }

            if (!responseFinal)
            {
                // Wire 3 deferred response: the broker accepted the request and owes a final
                // frame on this correlation, so the response timeout no longer applies.
                pending.Acknowledge();
                return;
            }

            _correlated.Remove(correlationId);
        }

        pending.CompleteResponse(payload.IsEmpty ? [] : payload.ToArray());
    }

    /// <summary>Queues a notification frame for every handler registered for its message type.</summary>
    /// <param name="messageType">Notification message type.</param>
    /// <param name="payload">Frame payload; copied once before queuing.</param>
    public void DispatchNotification(ushort messageType, ReadOnlyMemory<byte> payload)
    {
        NotificationDispatch dispatch;
        lock (_gate)
        {
            if (!_connected || !_notificationHandlerSnapshots.TryGetValue(messageType, out var handlers))
            {
                return;
            }

            dispatch = new NotificationDispatch(handlers, payload.IsEmpty ? [] : payload.ToArray(), _sessionEpoch);
            if (_notificationRestoreActive)
            {
                if (_restoringNotifications.Count >= RestoreBufferCapacity)
                {
                    _notificationRestoreFailure ??= new SubscriptionBackpressureException(
                        "The reconnect notification buffer is full.");
                    return;
                }

                _restoringNotifications.Enqueue(dispatch);
                return;
            }
        }

        if (!_notificationQueue.Writer.TryWrite(dispatch))
        {
            _onDispatchError(new SubscriptionBackpressureException(
                "The multiplexer notification dispatch queue is full."));
        }
    }

    /// <summary>Sends a correlated request frame and awaits its response.</summary>
    /// <param name="messageType">Request message type, used in diagnostics.</param>
    /// <param name="frameData">The encoded frame, already carrying <paramref name="correlationId"/>.</param>
    /// <param name="send">Writes the frame to the transport.</param>
    /// <param name="timeout">
    /// Deadline for the first response frame, or <see cref="Timeout.InfiniteTimeSpan"/>. Once a
    /// non-final frame arrives, only <paramref name="ct"/> and connection loss end the wait.
    /// </param>
    /// <param name="correlationId">Non-zero identifier the broker echoes on the response.</param>
    /// <param name="ct">Cancellation token for the request.</param>
    /// <returns>The response payload.</returns>
    public async Task<byte[]> RequestAsync(
        ushort messageType,
        byte[] frameData,
        Func<ReadOnlyMemory<byte>, CancellationToken, Task> send,
        TimeSpan timeout,
        ulong correlationId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(send);
        ArgumentOutOfRangeException.ThrowIfZero(correlationId);

        var request = new PendingRequest(this, messageType, correlationId, timeout, ct);
        lock (_gate)
        {
            if (_disposed || !_connected)
            {
                throw new ConnectionException("Connection closed or reset");
            }

            if (_correlationTombstones.Contains(correlationId) || !_correlated.TryAdd(correlationId, request))
            {
                throw new ProtocolException(
                    $"Correlation identifier {correlationId} is already in flight on this connection.");
            }
        }

        using var timeoutCts = timeout == Timeout.InfiniteTimeSpan ? null : new CancellationTokenSource(timeout, _time);
        using var cancellationRegistration = ct.CanBeCanceled
            ? ct.Register(static state => ((PendingRequest)state!).Cancel(), request)
            : default;
        using var timeoutRegistration = timeoutCts?.Token.Register(static state => ((PendingRequest)state!).Timeout(), request) ?? default;

        try
        {
            ct.ThrowIfCancellationRequested();
            await send(frameData, ct).ConfigureAwait(false);
        }
        catch
        {
            request.FailSend();
            throw;
        }

        return await request.Response.ConfigureAwait(false);
    }

    /// <summary>Fails every pending request and releases dispatch resources.</summary>
    public void Dispose()
    {
        SetDisconnected();
        _notificationQueue.Writer.TryComplete();

        lock (_gate)
        {
            _disposed = true;
            _notificationHandlers.Clear();
            _notificationHandlerSnapshots.Clear();
        }
    }

    internal async Task CompleteNotificationDispatchAsync(TimeSpan timeout, CancellationToken ct)
    {
        _notificationQueue.Writer.TryComplete();
        await _notificationPump.WaitAsync(timeout, _time, ct).ConfigureAwait(false);
    }

    async Task PumpNotificationsAsync()
    {
        await foreach (var notification in _notificationQueue.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            foreach (var handler in notification.Handlers)
            {
                lock (_gate)
                {
                    if (!_connected || notification.SessionEpoch != _sessionEpoch)
                    {
                        break;
                    }
                }

                try
                {
                    handler(notification.Payload);
                }
                catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
                {
                    // Isolation boundary: one failing subscriber must not stop delivery to the
                    // others, nor end the pump for every later frame.
                    _onDispatchError(error);
                }
            }
        }
    }

    void RemoveRequest(PendingRequest request)
    {
        List<PendingRequest>? reset = null;
        lock (_gate)
        {
            // Only drop the entry if it is still this request's: a late duplicate response must
            // not evict a newer request that reused the identifier.
            if (!_correlated.TryGetValue(request.CorrelationId, out var registered) || !ReferenceEquals(registered, request))
            {
                return;
            }

            _correlated.Remove(request.CorrelationId);
            if (!request.RetainCorrelationTombstone)
            {
                return;
            }

            if (_correlationTombstones.Count < _maxCorrelationTombstones)
            {
                _correlationTombstones.Add(request.CorrelationId);
                return;
            }

            // Evicting an arbitrary identifier could turn a valid late reply into an unknown
            // response, so reset the session rather than keep unbounded cancellation history.
            reset = ResetSessionUnsafe();
        }

        var failure = new ConnectionException("The canceled-request correlation history is full; the connection was reset.");
        FailAll(reset, failure);
        _onSessionReset(failure);
    }

    List<PendingRequest> ResetSessionUnsafe()
    {
        // Advancing the epoch makes the pump drop notifications queued for the ended session.
        _connected = false;
        _sessionEpoch++;
        ClearRestoreUnsafe();
        List<PendingRequest> pending = [.. _correlated.Values];
        _correlated.Clear();
        _correlationTombstones.Clear();
        return pending;
    }

    void ClearRestoreUnsafe()
    {
        _restoringNotifications.Clear();
        _notificationRestoreFailure = null;
        _notificationRestoreActive = false;
    }

    static void FailAll(List<PendingRequest> pending, ConnectionException failure)
    {
        foreach (var request in pending)
        {
            request.FailConnection(failure);
        }
    }

    void RebuildHandlerSnapshotUnsafe(ushort messageType, Dictionary<long, Action<ReadOnlyMemory<byte>>> registrations)
    {
        if (registrations.Count == 0)
        {
            _notificationHandlers.Remove(messageType);
            _notificationHandlerSnapshots.Remove(messageType);
            return;
        }

        _notificationHandlerSnapshots[messageType] = [.. registrations.Values];
    }

    void RemoveNotificationHandler(ushort messageType, long handlerId)
    {
        lock (_gate)
        {
            if (_notificationHandlers.TryGetValue(messageType, out var handlers) && handlers.Remove(handlerId))
            {
                RebuildHandlerSnapshotUnsafe(messageType, handlers);
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

        readonly Lock _stateGate = new();
        readonly Multiplexer _owner;
        readonly ushort _messageType;
        readonly TimeSpan _timeout;
        readonly CancellationToken _cancellationToken;
        readonly TaskCompletionSource<byte[]> _promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CompletionKind _completionKind;
        bool _acknowledged;

        internal PendingRequest(Multiplexer owner, ushort messageType, ulong correlationId, TimeSpan timeout, CancellationToken ct)
        {
            _owner = owner;
            _messageType = messageType;
            CorrelationId = correlationId;
            _timeout = timeout;
            _cancellationToken = ct;
        }

        internal ulong CorrelationId { get; }

        internal Task<byte[]> Response => _promise.Task;

        /// <summary>
        /// The broker may still answer a request abandoned after it was sent, so its identifier
        /// must be remembered.
        /// </summary>
        internal bool RetainCorrelationTombstone
        {
            get
            {
                lock (_stateGate)
                {
                    return _completionKind is CompletionKind.Canceled or CompletionKind.TimedOut;
                }
            }
        }

        internal void Cancel()
        {
            if (TryComplete(CompletionKind.Canceled))
            {
                _promise.TrySetCanceled(_cancellationToken);
                _owner.RemoveRequest(this);
            }
        }

        // A non-final frame arrived; the final one may legitimately take longer than the timeout.
        internal void Acknowledge()
        {
            lock (_stateGate)
            {
                _acknowledged = true;
            }
        }

        internal void Timeout()
        {
            lock (_stateGate)
            {
                if (_acknowledged)
                    return;
            }

            if (TryComplete(CompletionKind.TimedOut))
            {
                _promise.TrySetException(new RequestTimeoutException(
                    $"Request timeout for message type {_messageType} after {_timeout.TotalMilliseconds}ms"));
                _owner.RemoveRequest(this);
            }
        }

        internal void CompleteResponse(byte[] payload)
        {
            if (TryComplete(CompletionKind.Response))
            {
                _promise.TrySetResult(payload);
            }
        }

        internal void FailSend()
        {
            TryComplete(CompletionKind.SendFailed);
            _owner.RemoveRequest(this);

            // A timeout or cancellation can fault the promise while the send is still running.
            // The caller sees the send failure instead, so observe the promise's own outcome here.
            _ = _promise.Task.Exception;
        }

        internal void FailConnection(ConnectionException failure)
        {
            if (TryComplete(CompletionKind.ConnectionFailed))
            {
                _promise.TrySetException(failure);
            }
        }

        bool TryComplete(CompletionKind kind)
        {
            lock (_stateGate)
            {
                if (_completionKind != CompletionKind.Active)
                {
                    return false;
                }

                _completionKind = kind;
                return true;
            }
        }
    }

    /// <summary>
    /// One received notification frame and the handlers it fans out to. Queuing per frame rather
    /// than per handler keeps the pump queue's capacity a count of frames, so a burst to many
    /// subscribers does not consume capacity proportional to the subscriber count.
    /// </summary>
    readonly record struct NotificationDispatch(Action<ReadOnlyMemory<byte>>[] Handlers, byte[] Payload, long SessionEpoch);

    sealed class NotificationRegistration(Multiplexer owner, ushort messageType, long handlerId) : IDisposable
    {
        bool _disposed;

        public void Dispose()
        {
            if (!Interlocked.Exchange(ref _disposed, true))
            {
                owner.RemoveNotificationHandler(messageType, handlerId);
            }
        }
    }
}
