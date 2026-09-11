namespace Cntryl.Fitz.Abstractions.Domains.Queue;

/// <summary>
/// Enqueues work and reserves it for processing.
/// </summary>
public interface IQueueClient
{
    /// <summary>
    /// Appends a message to a queue.
    /// </summary>
    /// <param name="route">Concrete <c>queue://</c> route to enqueue on.</param>
    /// <param name="body">Opaque message payload.</param>
    /// <param name="delayMs">
    /// Delivery delay. Accepted only in whole seconds; a value that is not a whole number of
    /// seconds is rejected rather than silently rounded.
    /// </param>
    /// <param name="ct">Cancellation token for the enqueue.</param>
    /// <returns>The broker-assigned message identifier.</returns>
    Task<ulong> EnqueueAsync(
        string route,
        ReadOnlyMemory<byte> body,
        int? delayMs = null,
        CancellationToken ct = default
    );

    /// <summary>
    /// Reserves messages for processing, leasing them for a visibility window.
    /// </summary>
    /// <param name="route">
    /// An exact route or whole-segment pattern capable of matching three segments.
    /// </param>
    /// <param name="leaseSeconds">Visibility timeout for each reserved message.</param>
    /// <param name="batchSize">Maximum messages to reserve.</param>
    /// <param name="waitSeconds">
    /// How long to wait for a message when the queue is empty, using the broker-native
    /// RESERVE wait field. A broker that rejects the field fails the request; the client
    /// does not fall back to polling.
    /// </param>
    /// <param name="ct">Cancellation token for the reserve.</param>
    /// <returns>
    /// The reserved messages, each carrying its concrete matched route. Dispose any item you
    /// do not complete. If any item carries an invalid route the whole response fails;
    /// partial reservations are never returned.
    /// </returns>
    Task<IQueueReservedItem[]> ReserveAsync(
        string route,
        ulong leaseSeconds,
        int batchSize = 1,
        int? waitSeconds = null,
        CancellationToken ct = default
    );

    /// <summary>
    /// Subscribes to availability changes for queues matching a pattern.
    /// </summary>
    /// <param name="pattern">
    /// An exact route or whole-segment pattern capable of matching three segments.
    /// Wildcard patterns consume the broker's per-domain quota.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>
    /// A handle yielding availability events with ready, delayed, and inflight counts.
    /// </returns>
    Task<QueueSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default
    );
}

/// <summary>
/// Represents a reserved message from the queue lease operation.
/// </summary>
/// <remarks>
/// Complete the item or dispose it asynchronously. Disposal releases local lifecycle resources;
/// the current Fitz wire protocol does not provide a queue nack operation.
/// </remarks>
public interface IQueueReservedItem : IAsyncDisposable
{
    /// <summary>The concrete route this message was reserved from, never the request pattern.</summary>
    string Route { get; }

    /// <summary>The opaque message payload.</summary>
    ReadOnlyMemory<byte> Body { get; }

    /// <summary>
    /// Delivery attempt count, or <see cref="QueueItem.AttemptUnavailable"/> when the broker
    /// does not report one. The current wire protocol never reports it.
    /// </summary>
    uint Attempt { get; }

    /// <summary>
    /// Extends this message's visibility timeout.
    /// </summary>
    /// <param name="leaseSeconds">Additional seconds to hold the reservation.</param>
    /// <param name="ct">Cancellation token for the extend request.</param>
    /// <returns>A task that completes once the broker accepts the extension.</returns>
    Task ExtendAsync(ulong leaseSeconds, CancellationToken ct = default);

    /// <summary>
    /// Acknowledges the message, removing it from the queue permanently.
    /// </summary>
    /// <param name="ct">Cancellation token for the complete request.</param>
    /// <returns>A task that completes once the broker accepts the completion.</returns>
    Task CompleteAsync(CancellationToken ct = default);

    /// <summary>
    /// Acknowledges the message using an explicit completion token.
    /// </summary>
    /// <param name="token">Completion token issued with the reservation.</param>
    /// <param name="ct">Cancellation token for the complete request.</param>
    /// <returns>A task that completes once the broker accepts the completion.</returns>
    Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default);
}
