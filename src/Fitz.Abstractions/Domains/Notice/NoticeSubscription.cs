
namespace Cntryl.Fitz;

/// <summary>
/// A live subscription to notices matching a route or pattern.
/// </summary>
/// <remarks>
/// Enumerate with <c>await foreach</c>; dispose to unsubscribe. Delivery is bounded per
/// handle, and the registration is restored automatically after a reconnect.
/// </remarks>
public sealed class NoticeSubscription : SubscriptionHandle<NoticeMessage>
{
    /// <summary>
    /// Initializes a handle that delivers no notifications, for callers that only need the
    /// unsubscribe lifecycle.
    /// </summary>
    /// <param name="pattern">Route or pattern this subscription was registered with.</param>
    /// <param name="unsubscribe">Callback that withdraws the registration from the broker.</param>
    /// <param name="completion">Externally owned completion signal, or <see langword="null"/>.</param>
    public NoticeSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(pattern, EmptyNotifications(), unsubscribe, completion)
    {
    }

    /// <summary>
    /// Initializes a handle over a notification source.
    /// </summary>
    /// <param name="pattern">Route or pattern this subscription was registered with.</param>
    /// <param name="notifications">Bounded source of notifications for this subscription.</param>
    /// <param name="unsubscribe">Callback that withdraws the registration from the broker.</param>
    /// <param name="completion">Externally owned completion signal, or <see langword="null"/>.</param>
    public NoticeSubscription(
        string pattern, IAsyncEnumerable<NoticeMessage> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(pattern, notifications, unsubscribe, completion)
    {
    }
}
