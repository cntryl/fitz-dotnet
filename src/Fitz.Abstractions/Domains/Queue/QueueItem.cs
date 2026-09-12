namespace Cntryl.Fitz;

/// <summary>
/// Base class for a reserved queue message.
/// </summary>
/// <remarks>
/// Complete the item once handled, or dispose it to release local lifecycle resources.
/// Disposal is not a nack: the current Fitz wire protocol has no nack operation, so an
/// uncompleted item becomes visible again only when its lease expires.
/// </remarks>
public abstract class QueueItem : IQueueReservedItem
{
    /// <summary>Sentinel used when the current wire protocol does not provide an attempt count.</summary>
    public const uint AttemptUnavailable = 0;

    /// <summary>
    /// Initializes the reserved item.
    /// </summary>
    /// <param name="route">Concrete route the message was reserved from.</param>
    /// <param name="body">Opaque message payload.</param>
    /// <param name="attempt">Delivery attempt count, or <see cref="AttemptUnavailable"/>.</param>
    protected QueueItem(string route, ReadOnlyMemory<byte> body, uint attempt = AttemptUnavailable)
    {
        Route = route;
        Body = body;
        Attempt = attempt;
    }

    /// <summary>The concrete route this message was reserved from, never the request pattern.</summary>
    public string Route { get; }

    /// <summary>The opaque message payload.</summary>
    public ReadOnlyMemory<byte> Body { get; }

    /// <summary>
    /// Delivery attempt count, or <see cref="AttemptUnavailable"/> when the broker does not
    /// report one. The current wire protocol never reports it.
    /// </summary>
    public uint Attempt { get; }

    /// <summary>
    /// Extends this message's visibility timeout.
    /// </summary>
    /// <param name="lease">Additional seconds to hold the reservation.</param>
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
