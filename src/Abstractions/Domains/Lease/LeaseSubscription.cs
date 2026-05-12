using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public sealed class LeaseSubscription : SubscriptionHandle
{
    public LeaseSubscription(
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, unsubscribe)
    {
    }
}
