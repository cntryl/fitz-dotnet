namespace Cntryl.Fitz.Runtime;

// Subscription requests are serialized by the domain subscription gate. Replay runs
// under its routing lock so new notifications cannot overtake buffered notifications.
sealed class NotificationActivationBuffer(int capacity) : IDisposable
{
    // Retired IDs can continue arriving from the bounded multiplexer queue. Track which
    // IDs actually lost frames without retaining an unbounded history of retired IDs.
    const int MaxTrackedOverflowIds = 1024;
    readonly object _gate = new();
    readonly Queue<(ulong SubscriptionId, byte[] Payload)> _notifications = new();
    readonly HashSet<ulong> _overflowedSubscriptionIds = [];
    bool _active;
    string? _pattern;
    Exception? _failure;

    internal NotificationActivationBuffer Begin(string pattern)
    {
        lock (_gate)
        {
            _active = true;
            _pattern = pattern;
            return this;
        }
    }

    internal void Buffer(ulong subscriptionId, string route, ReadOnlyMemory<byte> payload)
    {
        lock (_gate)
        {
            if (!_active || _failure is not null || !RouteValidation.MatchesPattern(route, _pattern!))
                return;

            if (_overflowedSubscriptionIds.Contains(subscriptionId))
                return;

            if (_notifications.Count >= capacity)
            {
                if (_overflowedSubscriptionIds.Count >= MaxTrackedOverflowIds)
                    _failure = new SubscriptionBackpressureException("The subscription activation overflow-ID tracking limit was exceeded.");
                else
                    _overflowedSubscriptionIds.Add(subscriptionId);
                return;
            }

            _notifications.Enqueue((subscriptionId, payload.ToArray()));
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

    internal Exception? Replay(ulong subscriptionId, Action<ReadOnlyMemory<byte>> handler)
    {
        lock (_gate)
        {
            _active = false;
            var failure = _failure;
            if (failure is null && _overflowedSubscriptionIds.Contains(subscriptionId))
                failure = new SubscriptionBackpressureException("The subscription activation buffer is full.");
            if (failure is null)
            {
                while (_notifications.TryDequeue(out var notification))
                {
                    if (notification.SubscriptionId == subscriptionId)
                        handler(notification.Payload);
                }
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
            _pattern = null;
            _notifications.Clear();
            _overflowedSubscriptionIds.Clear();
            _failure = null;
        }
    }
}
