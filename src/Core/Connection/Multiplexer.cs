using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Connection;

public sealed class Multiplexer
{
    private readonly Dictionary<ushort, Queue<PendingRequest>> _pending = new();
    private readonly Dictionary<ushort, Action<byte[]>> _notificationHandlers = new();
    private readonly Dictionary<ushort, int> _optionalResponses = new();
    private ConnectionState _state = ConnectionState.Disconnected;

    public void SetConnected() => _state = ConnectionState.Authenticated;

    public void SetDisconnected()
    {
        _state = ConnectionState.Disconnected;
        _optionalResponses.Clear();
        CancelAll();
    }

    public void RegisterNotificationHandler(ushort messageType, Action<byte[]> handler)
    {
        _notificationHandlers[messageType] = handler;
    }

    public void UnregisterNotificationHandler(ushort messageType)
    {
        _notificationHandlers.Remove(messageType);
    }

    public Action ExpectOptionalResponse(ushort messageType)
    {
        _optionalResponses[messageType] = _optionalResponses.GetValueOrDefault(messageType) + 1;
        return () =>
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
        if (!_pending.TryGetValue(messageType, out var queue))
        {
            queue = new Queue<PendingRequest>();
            _pending[messageType] = queue;
        }

        queue.Enqueue(request);

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
            await send(frameData, cancellationToken);
            return await tcs.Task;
        }
        catch
        {
            RemovePending(messageType, request);
            throw;
        }
    }

    public void Dispatch(ushort messageType, byte[] payload)
    {
        if (_pending.TryGetValue(messageType, out var queue) && queue.Count > 0)
        {
            var pending = queue.Dequeue();
            if (queue.Count == 0)
            {
                _pending.Remove(messageType);
            }

            pending.Promise.TrySetResult(payload);
            return;
        }

        if (_notificationHandlers.TryGetValue(messageType, out var handler))
        {
            try
            {
                handler(payload);
            }
            catch
            {
            }

            return;
        }

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

    public void CancelAll()
    {
        foreach (var queue in _pending.Values)
        {
            foreach (var pending in queue)
            {
                pending.Promise.TrySetException(new ConnectionException("Connection closed or reset"));
            }
        }

        _pending.Clear();
    }

    private void RemovePending(ushort messageType, PendingRequest request)
    {
        if (!_pending.TryGetValue(messageType, out var queue) || queue.Count == 0)
        {
            return;
        }

        var arr = queue.ToArray().Where(x => !ReferenceEquals(x, request)).ToArray();
        if (arr.Length == 0)
        {
            _pending.Remove(messageType);
            return;
        }

        _pending[messageType] = new Queue<PendingRequest>(arr);
    }

    private sealed record PendingRequest(TaskCompletionSource<byte[]> Promise);
}