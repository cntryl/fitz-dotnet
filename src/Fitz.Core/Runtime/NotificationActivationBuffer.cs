namespace Cntryl.Fitz.Runtime;

// The subscription table holds its routing lock for staging and activation. Only a broker ACK
// assigns a staged ID to a selector. Matching the route merely avoids staging unrelated frames.
sealed class NotificationActivationBuffer<TNotification>(int capacity, int maxConcurrentChanges)
{
    const int MaxTrackedOverflowIds = 1024;
    readonly HashSet<string> _pending = [with(StringComparer.Ordinal)];
    readonly Dictionary<ulong, Queue<TNotification>> _byId = [];
    readonly HashSet<ulong> _overflowed = [];
    long _count;
    Exception? _failure;

    internal void Begin(string pattern) => _pending.Add(pattern);

    internal void Buffer(DecodedNotification<TNotification> notification)
    {
        if (_failure is not null || _overflowed.Contains(notification.SubscriptionId))
            return;

        foreach (var pattern in _pending)
        {
            if (RouteValidation.MatchesPattern(notification.Route, pattern))
            {
                BufferMatching(notification.SubscriptionId, notification.Value);
                return;
            }
        }
    }

    void BufferMatching(ulong id, TNotification value)
    {
        _byId.TryGetValue(id, out var frames);
        if (_count >= (long)capacity * maxConcurrentChanges || frames?.Count >= capacity)
        {
            if (_overflowed.Count >= MaxTrackedOverflowIds)
                _failure = new SubscriptionBackpressureException("The subscription activation overflow-ID tracking limit was exceeded.");
            else
                _overflowed.Add(id);
            if (_byId.Remove(id, out var dropped))
                _count -= dropped.Count;
            return;
        }

        if (frames is null)
        {
            frames = new Queue<TNotification>();
            _byId.Add(id, frames);
        }

        frames.Enqueue(value);
        _count++;
    }

    internal Exception? Take(ulong id, out Queue<TNotification>? frames)
    {
        if (_byId.Remove(id, out frames))
            _count -= frames.Count;

        return _failure ?? (_overflowed.Remove(id)
            ? new SubscriptionBackpressureException("The subscription activation buffer is full.")
            : null);
    }

    internal void End(string pattern)
    {
        _pending.Remove(pattern);
        if (_pending.Count == 0)
            Clear();
    }

    internal void Clear()
    {
        _pending.Clear();
        _byId.Clear();
        _overflowed.Clear();
        _count = 0;
        _failure = null;
    }
}
