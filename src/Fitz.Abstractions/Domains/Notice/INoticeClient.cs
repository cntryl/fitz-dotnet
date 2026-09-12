namespace Cntryl.Fitz;

/// <summary>
/// Publishes and subscribes to notices: fire-and-forget messages delivered to every
/// current subscriber, with no persistence and no delivery guarantee.
/// </summary>
public interface INoticeClient
{
    /// <summary>
    /// Publishes a notice to an exact route.
    /// </summary>
    /// <param name="route">Concrete <c>notice://</c> route. Patterns are not accepted.</param>
    /// <param name="body">Opaque payload delivered verbatim to subscribers.</param>
    /// <param name="ct">Cancellation token for the publish.</param>
    /// <returns>A task that completes once the broker accepts the notice.</returns>
    Task PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);

    /// <summary>
    /// Subscribes to notices matching a pattern.
    /// </summary>
    /// <param name="pattern">
    /// An exact route or a whole-segment <c>*</c>/<c>**</c> pattern. Notice patterns have
    /// flexible depth, so they need not match a fixed segment count.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>
    /// A handle that yields notices by <c>await foreach</c> and unsubscribes on disposal.
    /// Each notification carries the exact concrete route that matched.
    /// </returns>
    Task<NoticeSubscription> SubscribeAsync(string pattern, CancellationToken ct = default);
}
