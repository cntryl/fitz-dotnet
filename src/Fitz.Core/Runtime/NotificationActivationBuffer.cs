namespace Cntryl.Fitz.Runtime;

// Subscription requests are serialized by the domain subscription gate. Replay runs
// under its routing lock so new notifications cannot overtake buffered notifications.
sealed class NotificationActivationBuffer(int capacity) : IDisposable
{
    readonly object _gate = new();
    readonly Queue<byte[]> _notifications = new();
    bool _active;
    Exception? _failure;

    internal NotificationActivationBuffer Begin()
    {
        lock (_gate)
        {
            _active = true;
            return this;
        }
    }

    internal void Buffer(ReadOnlyMemory<byte> payload)
    {
        lock (_gate)
        {
            if (!_active || _failure is not null)
                return;

            if (_notifications.Count >= capacity)
            {
                _failure = new SubscriptionBackpressureException("The subscription activation buffer is full.");
                return;
            }

            _notifications.Enqueue(payload.ToArray());
        }
    }

    internal void Fail(Exception error)
    {
        lock (_gate)
        {
            if (_active)
                _failure ??= error;
        }
    }

    internal Exception? Replay(Action<ReadOnlyMemory<byte>> handler)
    {
        lock (_gate)
        {
            _active = false;
            var failure = _failure;
            if (failure is null)
            {
                while (_notifications.TryDequeue(out var payload))
                    handler(payload);
            }
            Dispose();
            return failure;
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _active = false;
            _notifications.Clear();
            _failure = null;
        }
    }
}
