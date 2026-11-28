namespace Cntryl.Fitz;

/// <summary>
/// Appends to and reads from append-only streams.
/// </summary>
public interface IStreamClient
{
    /// <summary>Deletes committed Stream data within the selected scope.</summary>
    /// <param name="selector">A realm wildcard, area wildcard, or exact resource selector.</param>
    /// <param name="mode">The required persistence policy for the deletion.</param>
    /// <param name="ct">Cancellation token for dispatch and waiting for the response.</param>
    /// <returns>A task that completes when the broker acknowledges the deletion.</returns>
    /// <remarks>
    /// The outcome can be indeterminate after dispatch if the response is lost.
    /// Reconcile the selected scope before retrying.
    /// Discard checkpoints for offsets reset by the deletion.
    /// </remarks>
    Task DropAsync(string selector, StreamCommitMode mode, CancellationToken ct = default) =>
        throw new NotSupportedException("This Stream client does not implement scoped DROP.");

    /// <summary>
    /// Opens an append session for a stream.
    /// </summary>
    /// <param name="route">Concrete <c>stream://</c> route to append to.</param>
    /// <param name="ingestMetadata">Optional opaque metadata describing the whole session.</param>
    /// <param name="ct">Cancellation token for the begin request.</param>
    /// <returns>A session whose appends become visible only on commit.</returns>
    Task<IStreamSession> BeginAsync(string route, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default);

    /// <summary>Reads batches in selector order, including filtered and empty batches.</summary>
    /// <param name="route">A resource, area, realm, or global stream selector.</param>
    /// <param name="fromOffset">Inclusive starting position on the selector's cursor axis.</param>
    /// <param name="mode">Replay a snapshot or follow subsequent commits.</param>
    /// <param name="options">Optional per-batch limits and filter.</param>
    /// <param name="ct">Cancellation token for the read and any subsequent wait.</param>
    /// <returns>Batches with the next offset to persist after processing each batch.</returns>
    IAsyncEnumerable<StreamReadBatch> ReadAsync(string route, ulong fromOffset, StreamReadMode mode, StreamReadOptions? options = null, CancellationToken ct = default);

    /// <summary>
    /// Reads the most recent record on a stream without consuming it.
    /// </summary>
    /// <param name="route">Concrete route. Patterns are not accepted for this operation.</param>
    /// <param name="ct">Cancellation token for the read.</param>
    /// <returns>The latest record, or <see langword="null"/> when the stream is empty.</returns>
    Task<StreamRecord?> PeekAsync(string route, CancellationToken ct = default);

    /// <summary>
    /// Reads stream metadata, including the current sequence position.
    /// </summary>
    /// <param name="route">Concrete route to inspect.</param>
    /// <param name="ct">Cancellation token for the request.</param>
    /// <returns>The stream's current metadata.</returns>
    Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default);

    /// <summary>
    /// Subscribes to commits on streams matching a pattern.
    /// </summary>
    /// <param name="pattern">
    /// An exact route or whole-segment selector capable of matching three segments.
    /// Wildcard patterns consume the broker's per-domain quota.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>
    /// A handle that yields commit events by <c>await foreach</c> and unsubscribes on disposal.
    /// </returns>
    Task<StreamSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default
    );
}
