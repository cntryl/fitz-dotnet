using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public sealed class LeaseSubscription : SubscriptionHandle
{
    public LeaseSubscription(
        string route,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(route, unsubscribe)
    {
        Route = route;
    }

    public string Route { get; }
}
