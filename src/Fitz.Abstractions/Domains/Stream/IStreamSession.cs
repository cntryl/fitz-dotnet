namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// An open stream append session. Appended records become visible only on commit.
/// </summary>
/// <remarks>
/// Disposing an uncommitted session rolls it back. Commit becomes terminal only after the
/// broker accepts it; an ambiguous failure leaves the session retryable.
/// </remarks>
public interface IStreamSession : IAsyncDisposable
{
    /// <summary>
    /// Appends a record to the session.
    /// </summary>
    /// <param name="expectedOffset">
    /// Sequence position this append must land at, for optimistic concurrency.
    /// </param>
    /// <param name="body">Opaque record payload.</param>
    /// <param name="metadata">Optional opaque metadata stored alongside the record.</param>
    /// <param name="discriminator">Optional subtype tag used by stream filters.</param>
    /// <param name="ct">Cancellation token for the append.</param>
    /// <returns>
    /// The assigned sequence position, or <see langword="null"/> when the broker defers
    /// assignment until commit.
    /// </returns>
    Task<ulong?> AppendAsync(ulong expectedOffset, ReadOnlyMemory<byte> body, ReadOnlyMemory<byte>? metadata = null, string? discriminator = null, CancellationToken ct = default);

    /// <summary>
    /// Commits the session, publishing every appended record.
    /// </summary>
    /// <param name="ct">Cancellation token for the commit.</param>
    /// <returns>A task that completes once the broker accepts the commit.</returns>
    Task CommitAsync(CancellationToken ct = default);

    /// <summary>
    /// Abandons the session and discards every appended record.
    /// </summary>
    /// <param name="ct">Cancellation token for the rollback.</param>
    /// <returns>A task that completes once the broker accepts the rollback.</returns>
    Task RollbackAsync(CancellationToken ct = default);
}
