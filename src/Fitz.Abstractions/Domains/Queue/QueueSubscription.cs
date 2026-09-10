using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public sealed class QueueSubscription : SubscriptionHandle<QueueAvailabilityEvent>
{
    public QueueSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(pattern, EmptyNotifications(), unsubscribe, completion)
    {
    }

    public QueueSubscription(
        string pattern, IAsyncEnumerable<QueueAvailabilityEvent> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(pattern, notifications, unsubscribe, completion)
    {
    }
}
