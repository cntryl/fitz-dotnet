using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public sealed class QueueSubscription : SubscriptionHandle<QueueAvailabilityEvent>
{
    public QueueSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe)
        : this(pattern, EmptyNotifications(), unsubscribe)
    {
    }

    public QueueSubscription(
        string pattern, IAsyncEnumerable<QueueAvailabilityEvent> notifications,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, notifications, unsubscribe)
    {
    }
}
