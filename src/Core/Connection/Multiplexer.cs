using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Connection;

public sealed class Multiplexer
{
    private readonly object _gate = new();
    private readonly Dictionary<ushort, LinkedList<PendingRequest>> _pending = new();
    private readonly Dictionary<ushort, Action<byte[]>> _notificationHandlers = new();
    private readonly Dictionary<ushort, int> _optionalResponses = new();
    private ConnectionState _state = ConnectionState.Disconnected;

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

    public void RegisterNotificationHandler(ushort messageType, Action<byte[]> handler)
    {
        lock (_gate)
        {
            _notificationHandlers[messageType] = handler;
        }
    }

    public void UnregisterNotificationHandler(ushort messageType)
    {
        lock (_gate)
        {
            _notificationHandlers.Remove(messageType);
        }
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
        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new PendingRequest(tcs);
        lock (_gate)
        {
            if (!_pending.TryGetValue(messageType, out var queue))
            {
                queue = new LinkedList<PendingRequest>();
                _pending[messageType] = queue;
            }

            request.Node = queue.AddLast(request);
        }

        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
        using var reg = linkedCts.Token.Register(() =>
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
    }

    public void Dispatch(ushort messageType, byte[] payload)
    {
        PendingRequest? pending = null;
        Action<byte[]>? notificationHandler = null;

        lock (_gate)
        {
            if (_pending.TryGetValue(messageType, out var queue) && queue.Count > 0)
            {
                var first = queue.First;
                if (first is not null)
                {
                    pending = first.Value;
                    pending.Node = null;
                    queue.RemoveFirst();
                }

                if (queue.Count == 0)
                {
                    _pending.Remove(messageType);
                }
            }
            else if (_notificationHandlers.TryGetValue(messageType, out var handler))
            {
                notificationHandler = handler;
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

        if (notificationHandler is not null)
        {
            try
            {
                notificationHandler(payload);
            }
            catch
            {
            }

            return;
        }
    }

    public void CancelAll()
    {
        PendingRequest[] requests;
        lock (_gate)
        {
            requests = _pending.Values.SelectMany(q => q).ToArray();
            foreach (var pending in requests)
            {
                pending.Node = null;
            }

            _pending.Clear();
        }

        foreach (var pending in requests)
        {
            pending.Promise.TrySetException(new ConnectionException("Connection closed or reset"));
        }
    }

    private void RemovePending(ushort messageType, PendingRequest request)
    {
        lock (_gate)
        {
            if (!_pending.TryGetValue(messageType, out var queue) || queue.Count == 0)
            {
                return;
            }

            var node = request.Node;
            if (node is null)
            {
                return;
            }

            queue.Remove(node);
            request.Node = null;
            if (queue.Count == 0)
            {
                _pending.Remove(messageType);
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

        internal LinkedListNode<PendingRequest>? Node { get; set; }
    }
}