using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public sealed class LeaseSubscription : SubscriptionHandle<LeaseChangeEvent>
{
    public LeaseSubscription(string route, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(route, EmptyNotifications(), unsubscribe, completion)
    {
    }

    public LeaseSubscription(
        string route, IAsyncEnumerable<LeaseChangeEvent> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(route, notifications, unsubscribe, completion)
    {
        Route = route;
    }

    public string Route { get; }
}
