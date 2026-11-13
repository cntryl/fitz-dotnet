namespace Cntryl.Fitz;

/// <summary>
/// Base class for a reserved queue message.
/// </summary>
/// <remarks>
/// Complete the item once handled, or dispose it to release local lifecycle resources.
/// Disposal releases local resources. Call NackAsync explicitly to relinquish a live reservation;
/// otherwise an uncompleted item becomes visible again when its lease expires.
/// </remarks>
public abstract class QueueItem : IQueueReservedItem
{
    /// <summary>Relinquishes the reservation with a nonnegative whole-second visibility delay.</summary>
    /// <param name="visibilityDelay">Delay before the next reservation, or zero for immediate visibility.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task completing after the broker acknowledges NACK.</returns>
    public abstract Task NackAsync(TimeSpan visibilityDelay = default, CancellationToken ct = default);
    /// <summary>
    /// Initializes the reserved item.
    /// </summary>
    /// <param name="route">Concrete route the message was reserved from.</param>
    /// <param name="body">Opaque message payload.</param>
    protected QueueItem(string route, ReadOnlyMemory<byte> body)
    {
        Route = route;
        Body = body;
    }

    /// <summary>The concrete route this message was reserved from, never the request pattern.</summary>
    public string Route { get; }

    /// <summary>The opaque message payload.</summary>
    public ReadOnlyMemory<byte> Body { get; }

    /// <summary>
    /// Extends this message's visibility timeout.
    /// </summary>
    /// <param name="lease">Additional time to hold the reservation. Must be a whole number of seconds.</param>
    /// <param name="ct">Cancellation token for the extend request.</param>
    /// <returns>A task that completes once the broker accepts the extension.</returns>
    public abstract Task ExtendAsync(TimeSpan lease, CancellationToken ct = default);

    /// <summary>
    /// Acknowledges the message, removing it from the queue permanently.
    /// </summary>
    /// <param name="ct">Cancellation token for the complete request.</param>
    /// <returns>A task that completes once the broker accepts the completion.</returns>
    /// <remarks>Completion becomes terminal only after broker success.</remarks>
    public abstract Task CompleteAsync(CancellationToken ct = default);

    /// <summary>
    /// Acknowledges the message using an explicit completion token.
    /// </summary>
    /// <param name="token">Completion token issued with the reservation.</param>
    /// <param name="ct">Cancellation token for the complete request.</param>
    /// <returns>A task that completes once the broker accepts the completion.</returns>
    public abstract Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default);

    /// <summary>
    /// Releases local lifecycle resources for this reservation.
    /// </summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    public abstract ValueTask DisposeAsync();
}
