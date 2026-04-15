using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public sealed class QueueSubscription : SubscriptionHandle
{
    public QueueSubscription(
        ulong subscriptionId,
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(subscriptionId, pattern, unsubscribe)
    {
    }
}