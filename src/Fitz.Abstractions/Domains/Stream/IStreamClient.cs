namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// Appends to and reads from append-only streams.
/// </summary>
public interface IStreamClient
{
    /// <summary>
    /// Opens an append session for a stream.
    /// </summary>
    /// <param name="route">Concrete <c>stream://</c> route to append to.</param>
    /// <param name="ingestMetadata">Optional opaque metadata describing the whole session.</param>
    /// <param name="ct">Cancellation token for the begin request.</param>
    /// <returns>A session whose appends become visible only on commit.</returns>
    Task<IStreamSession> BeginAsync(string route, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default);

    /// <summary>
    /// Reads records from a sequence position, following continuation cursors automatically.
    /// </summary>
    /// <param name="route">
    /// A concrete route, <c>realm/area/*</c>, <c>realm/*/*</c>, or <c>stream://**</c>.
    /// </param>
    /// <param name="startOffset">Sequence position to start from.</param>
    /// <param name="limit">Maximum records per underlying page request.</param>
    /// <param name="filter">Optional server-side filter applied to each record.</param>
    /// <param name="maxBytes">Optional cap on the bytes returned per page.</param>
    /// <param name="cursorFingerprint">Cursor fingerprint used to validate continuation.</param>
    /// <param name="capturedWatermark">Watermark captured when the read began.</param>
    /// <param name="ct">Cancellation token for the read.</param>
    /// <returns>
    /// Records in sequence order, paging until the stream reports no more. Every record
    /// carries its concrete matched route. Use <see cref="ReadPageAsync"/> when the caller
    /// needs explicit page boundaries.
    /// </returns>
    IAsyncEnumerable<StreamRecord> ReadAsync(string route, ulong startOffset, ulong limit = 100, StreamFilterSet? filter = null, ulong? maxBytes = null, ulong? cursorFingerprint = null, ulong? capturedWatermark = null, CancellationToken ct = default);

    /// <summary>
    /// Reads a single page of records, exposing the continuation cursor.
    /// </summary>
    /// <param name="route">
    /// A concrete route, <c>realm/area/*</c>, <c>realm/*/*</c>, or <c>stream://**</c>.
    /// </param>
    /// <param name="startOffset">Sequence position to start from.</param>
    /// <param name="limit">Maximum records to return.</param>
    /// <param name="filter">Optional server-side filter applied to each record.</param>
    /// <param name="maxBytes">Optional cap on the bytes returned.</param>
    /// <param name="cursorFingerprint">Cursor fingerprint used to validate continuation.</param>
    /// <param name="capturedWatermark">Watermark captured when the read began.</param>
    /// <param name="ct">Cancellation token for the read.</param>
    /// <returns>
    /// One page, its continuation cursor, and whether more records follow. If any item
    /// carries an invalid concrete route the whole page fails; partial batches are never
    /// returned.
    /// </returns>
    Task<StreamReadPage> ReadPageAsync(string route, ulong startOffset, ulong limit = 100, StreamFilterSet? filter = null, ulong? maxBytes = null, ulong? cursorFingerprint = null, ulong? capturedWatermark = null, CancellationToken ct = default);

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
        CancellationToken ct = default);
}
