namespace Cntryl.Fitz.Abstractions.Domains.Kv;

/// <summary>
/// Represents an active KV transaction.
/// </summary>
public interface IKvTransaction : IAsyncDisposable
{
    /// <summary>
    /// Reads a key from the transaction snapshot.
    /// </summary>
    /// <param name="key">Key bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Get result including found state and value.</returns>
    Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default);

    /// <summary>
    /// Upserts a key/value pair.
    /// </summary>
    /// <param name="key">Key bytes.</param>
    /// <param name="value">Value bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default);

    /// <summary>
    /// Inserts a key/value pair and fails if the key already exists.
    /// </summary>
    /// <param name="key">Key bytes.</param>
    /// <param name="value">Value bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default);

    /// <summary>
    /// Deletes a key.
    /// </summary>
    /// <param name="key">Key bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default);

    /// <summary>
    /// Deletes a half-open key range [startKey, endKey).
    /// </summary>
    /// <param name="startKey">Inclusive range start key.</param>
    /// <param name="endKey">Exclusive range end key.</param>
    /// <param name="ct">Cancellation token.</param>
    Task DeleteRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, CancellationToken ct = default);

    /// <summary>
    /// Streams key/value pairs for a scan query.
    /// </summary>
    /// <param name="query">Scan query options.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Async sequence of KV pairs.</returns>
    IAsyncEnumerable<KvPair> ScanAsync(KvScanQuery query, CancellationToken ct = default);

    /// <summary>
    /// Commits the transaction.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task CommitAsync(CancellationToken ct = default);

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task RollbackAsync(CancellationToken ct = default);
}
