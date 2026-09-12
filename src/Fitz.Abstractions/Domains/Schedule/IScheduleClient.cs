namespace Cntryl.Fitz;

/// <summary>
/// Registers cron schedules and subscribes to their firings.
/// </summary>
public interface IScheduleClient
{
    /// <summary>
    /// Registers a schedule that delivers a payload to a route on a cron cadence.
    /// </summary>
    /// <param name="route">Concrete <c>schedule://</c> route to deliver to.</param>
    /// <param name="cron">Cron expression describing the cadence.</param>
    /// <param name="deliveryMode">Whether each firing goes to every subscriber or exactly one.</param>
    /// <param name="payload">Opaque payload delivered on each firing.</param>
    /// <param name="ct">Cancellation token for the create request.</param>
    /// <returns>
    /// The broker-assigned schedule identifier, or <see langword="null"/> when the broker
    /// accepts the schedule without assigning one.
    /// </returns>
    Task<string?> CreateAsync(string route, string cron, ScheduleDeliveryMode deliveryMode, ReadOnlyMemory<byte> payload, CancellationToken ct = default);

    /// <summary>
    /// Removes a registered schedule.
    /// </summary>
    /// <param name="route">Concrete route of the schedule to cancel.</param>
    /// <param name="ct">Cancellation token for the cancel request.</param>
    /// <returns>A task that completes once the broker removes the schedule.</returns>
    Task CancelAsync(string route, CancellationToken ct = default);

    /// <summary>
    /// Reads one page of registered schedules.
    /// </summary>
    /// <param name="offset">Zero-based index of the first entry to return. Defaults to the start.</param>
    /// <param name="limit">Maximum entries to return. Defaults to the broker's page size.</param>
    /// <param name="ct">Cancellation token for the list request.</param>
    /// <returns>The page of entries together with the total schedule count.</returns>
    Task<ScheduleListPage> ListAsync(ulong? offset = null, ulong? limit = null, CancellationToken ct = default);

    /// <summary>
    /// Reads every registered schedule whose route matches a selector.
    /// </summary>
    /// <param name="selector">An exact route or whole-segment pattern to match.</param>
    /// <param name="ct">Cancellation token for the list request.</param>
    /// <returns>The matching entries.</returns>
    Task<IReadOnlyList<ScheduleEntry>> ListBySelectorAsync(string selector, CancellationToken ct = default);

    /// <summary>
    /// Subscribes to schedule firings matching a pattern.
    /// </summary>
    /// <param name="pattern">
    /// An exact route or a whole-segment <c>*</c>/<c>**</c> pattern capable of matching four
    /// segments. Wildcard patterns consume the broker's per-domain quota.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>
    /// A handle that yields firings by <c>await foreach</c> and unsubscribes on disposal.
    /// </returns>
    Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default);
}
