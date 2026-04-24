using Cntryl.Fitz;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Connection;

public sealed class Multiplexer
{
    private readonly object _gate = new();
    private readonly Dictionary<ushort, PendingRequest> _pending = new();
    private readonly Dictionary<ushort, SemaphoreSlim> _requestLanes = new();
    private readonly Dictionary<ushort, Dictionary<long, NotificationHandler>> _notificationHandlers = new();
    private readonly Dictionary<ushort, int> _optionalResponses = new();
    private ConnectionState _state = ConnectionState.Disconnected;
    private long _nextHandlerId;

    public void SetConnected()
    {
        lock (_gate)
        {
            _state = ConnectionState.Authenticated;
        }
    }

    public void SetDisconnected()
    {
        lock (_gate)
        {
            _state = ConnectionState.Disconnected;
            _optionalResponses.Clear();
        }

        CancelAll();
    }

    public IDisposable RegisterNotificationHandler(ushort messageType, Action<byte[]> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return RegisterNotificationHandlerCore(messageType, NotificationHandler.FromOwned(handler));
    }

    internal IDisposable RegisterBorrowedNotificationHandler(ushort messageType, Action<ReadOnlyMemory<byte>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        return RegisterNotificationHandlerCore(messageType, NotificationHandler.FromBorrowed(handler));
    }

    private IDisposable RegisterNotificationHandlerCore(ushort messageType, NotificationHandler handler)
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

    public void Dispatch(ushort messageType, ReadOnlyMemory<byte> payload)
    {
        PendingRequest? pending = null;
        NotificationHandler[]? handlers = null;

        lock (_gate)
        {
            if (_pending.TryGetValue(messageType, out pending))
            {
                _pending.Remove(messageType);
            }
            else if (_notificationHandlers.TryGetValue(messageType, out var registeredHandlers) && registeredHandlers.Count > 0)
            {
                handlers = new NotificationHandler[registeredHandlers.Count];
                registeredHandlers.Values.CopyTo(handlers, 0);
            }
            else
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

    public void Dispatch(ushort messageType, byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        Dispatch(messageType, payload.AsMemory());
    }

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

    public async Task<byte[]> RequestAsync(
        ushort messageType,
        byte[] frameData,
        Func<ReadOnlyMemory<byte>, CancellationToken, Task> send,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        var lane = GetLane(messageType);
        await lane.WaitAsync(cancellationToken).ConfigureAwait(false);

        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new PendingRequest(this, messageType, timeout, cancellationToken, tcs);

        using var timeoutCts = timeout == Timeout.InfiniteTimeSpan ? null : new CancellationTokenSource(timeout);
        using var cancellationRegistration = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => ((PendingRequest)state!).Cancel(), request)
            : default;
        using var timeoutRegistration = timeoutCts?.Token.Register(static state => ((PendingRequest)state!).Timeout(), request) ?? default;

        lock (_gate)
        {
            if (_pending.ContainsKey(messageType))
            {
                throw new InvalidOperationException($"A request is already pending for message type {messageType}.");
            }

            _pending[messageType] = request;
        }

        try
        {
            await send(frameData, cancellationToken).ConfigureAwait(false);
            return await tcs.Task.ConfigureAwait(false);
        }
        catch
        {
            RemovePending(messageType, request);
            throw;
        }
        finally
        {
            lane.Release();
        }
    }

    public void CancelAll()
    {
        PendingRequest[] pending;
        lock (_gate)
        {
            pending = new PendingRequest[_pending.Count];
            _pending.Values.CopyTo(pending, 0);
            _pending.Clear();
        }

        foreach (var request in pending)
        {
            request.Promise.TrySetException(new ConnectionException("Connection closed or reset"));
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

    private void RemovePending(ushort messageType, PendingRequest request)
    {
        lock (_gate)
        {
            if (_pending.TryGetValue(messageType, out var existing) && ReferenceEquals(existing, request))
            {
                _pending.Remove(messageType);
            }
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

        internal PendingRequest(
            Multiplexer owner,
            ushort messageType,
            TimeSpan timeout,
            CancellationToken cancellationToken,
            TaskCompletionSource<byte[]> promise)
        {
            _owner = owner;
            _messageType = messageType;
            _timeout = timeout;
            _cancellationToken = cancellationToken;
            Promise = promise;
        }

        internal TaskCompletionSource<byte[]> Promise { get; }

        internal void Cancel()
        {
            _owner.RemovePending(_messageType, this);
            Promise.TrySetCanceled(_cancellationToken);
        }

        internal void Timeout()
        {
            _owner.RemovePending(_messageType, this);
            Promise.TrySetException(new RequestTimeoutException($"Request timeout for message type {_messageType} after {_timeout.TotalMilliseconds}ms"));
        }
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
