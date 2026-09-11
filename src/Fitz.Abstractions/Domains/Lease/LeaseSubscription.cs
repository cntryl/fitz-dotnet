using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>
/// A live subscription to ownership changes for one exact lease route.
/// </summary>
/// <remarks>
/// Enumerate with <c>await foreach</c>; dispose to unsubscribe. Delivery is bounded per
/// handle, and the registration is restored automatically after a reconnect.
/// </remarks>
public sealed class LeaseSubscription : SubscriptionHandle<LeaseChangeEvent>
{
    /// <summary>
    /// Initializes a handle that delivers no notifications, for callers that only need the
    /// unsubscribe lifecycle.
    /// </summary>
    /// <param name="route">Route or pattern this subscription was registered with.</param>
    /// <param name="unsubscribe">Callback that withdraws the registration from the broker.</param>
    /// <param name="completion">Externally owned completion signal, or <see langword="null"/>.</param>
    public LeaseSubscription(string route, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(route, EmptyNotifications(), unsubscribe, completion)
    {
    }

    /// <summary>
    /// Initializes a handle over a notification source.
    /// </summary>
    /// <param name="route">Route or pattern this subscription was registered with.</param>
    /// <param name="notifications">Bounded source of notifications for this subscription.</param>
    /// <param name="unsubscribe">Callback that withdraws the registration from the broker.</param>
    /// <param name="completion">Externally owned completion signal, or <see langword="null"/>.</param>
    public LeaseSubscription(
        string route, IAsyncEnumerable<LeaseChangeEvent> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(route, notifications, unsubscribe, completion)
    {
        Route = route;
    }

    /// <summary>The exact lease route this subscription watches.</summary>
    public string Route { get; }
}
