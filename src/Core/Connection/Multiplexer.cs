using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Connection;

public sealed class Multiplexer
{
    private readonly object _gate = new();
    private readonly Dictionary<ushort, PendingRequest> _pending = new();
    private readonly Dictionary<ushort, SemaphoreSlim> _requestLanes = new();
    private readonly Dictionary<ushort, Dictionary<long, Action<byte[]>>> _notificationHandlers = new();
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

        long handlerId;
        lock (_gate)
        {
            handlerId = ++_nextHandlerId;
            if (!_notificationHandlers.TryGetValue(messageType, out var handlers))
            {
                handlers = new Dictionary<long, Action<byte[]>>();
                _notificationHandlers[messageType] = handlers;
            }

            handlers[handlerId] = handler;
        }

        return new NotificationRegistration(this, messageType, handlerId);
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
        var request = new PendingRequest(tcs);

        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
        using var registration = linkedCts.Token.Register(() =>
        {
            RemovePending(messageType, request);
            if (timeoutCts.IsCancellationRequested)
            {
                tcs.TrySetException(new RequestTimeoutException($"Request timeout for message type {messageType} after {timeout.TotalMilliseconds}ms"));
            }
            else
            {
                tcs.TrySetCanceled(cancellationToken);
            }
        });

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

    public void Dispatch(ushort messageType, byte[] payload)
    {
        PendingRequest? pending = null;
        Action<byte[]>[]? handlers = null;

        lock (_gate)
        {
            if (_pending.TryGetValue(messageType, out pending))
            {
                _pending.Remove(messageType);
            }
            else if (_notificationHandlers.TryGetValue(messageType, out var registeredHandlers) && registeredHandlers.Count > 0)
            {
                handlers = registeredHandlers.Values.ToArray();
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
            pending.Promise.TrySetResult(payload);
            return;
        }

        if (handlers is null)
        {
            return;
        }

        foreach (var handler in handlers)
        {
            try
            {
                handler(payload);
            }
            catch
            {
            }
        }
    }

    public void CancelAll()
    {
        PendingRequest[] pending;
        lock (_gate)
        {
            pending = _pending.Values.ToArray();
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
        internal PendingRequest(TaskCompletionSource<byte[]> promise)
        {
            Promise = promise;
        }

        internal TaskCompletionSource<byte[]> Promise { get; }
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
