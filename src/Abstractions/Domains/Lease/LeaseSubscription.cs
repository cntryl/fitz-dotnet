using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public sealed class LeaseSubscription : SubscriptionHandle<LeaseChangeEvent>
{
    public LeaseSubscription(string route, Func<CancellationToken, ValueTask> unsubscribe)
        : this(route, EmptyNotifications(), unsubscribe)
    {
    }

    public LeaseSubscription(
        string route, IAsyncEnumerable<LeaseChangeEvent> notifications,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(route, notifications, unsubscribe)
    {
        Route = route;
    }

    public string Route { get; }
}
