using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public sealed class LeaseSubscription : SubscriptionHandle
{
    public LeaseSubscription(
        ulong subscriptionId,
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(subscriptionId, pattern, unsubscribe)
    {
    }
}