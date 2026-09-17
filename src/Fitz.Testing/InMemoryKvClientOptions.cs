namespace Cntryl.Fitz.Testing;

/// <summary>Configures the deterministic behavior of <see cref="InMemoryKvClient"/>.</summary>
public sealed record InMemoryKvClientOptions
{
    /// <summary>
    /// Gets the maximum number of pairs returned by one scan, even when the query asks for
    /// more. A value of <see langword="null"/> leaves scans limited only by their query.
    /// </summary>
    public int? ScanPageSize { get; init; }

    /// <summary>
    /// Gets the maximum buffered notifications per subscription. A value of
    /// <see langword="null"/> uses an unbounded test channel.
    /// </summary>
    public int? SubscriptionBufferCapacity { get; init; }
}

/// <summary>Identifies an operation recorded or faulted by <see cref="InMemoryKvClient"/>.</summary>
public enum KvTestOperation
{
    /// <summary>Begin a transaction.</summary>
    Begin,
    /// <summary>Read one key.</summary>
    Get,
    /// <summary>Upsert one key.</summary>
    Put,
    /// <summary>Insert one key.</summary>
    Insert,
    /// <summary>Delete one key.</summary>
    Delete,
    /// <summary>Delete a key range.</summary>
    DeleteRange,
    /// <summary>Scan a key range.</summary>
    Scan,
    /// <summary>Commit a transaction.</summary>
    Commit,
    /// <summary>Roll back a transaction.</summary>
    Rollback,
    /// <summary>Subscribe to mutations.</summary>
    Subscribe,
}

/// <summary>Describes one operation observed by an in-memory KV client.</summary>
/// <param name="TransactionId">Stable test-local transaction identifier, when applicable.</param>
/// <param name="Operation">Operation kind.</param>
/// <param name="Route">Exact route or subscription pattern used by the operation.</param>
/// <param name="Durability">Requested durability, for a begin operation.</param>
/// <param name="Mode">Transaction mode, when applicable.</param>
/// <param name="Key">Cloned primary key, when applicable.</param>
/// <param name="Value">Cloned value, when applicable.</param>
/// <param name="EndKey">Cloned exclusive end key, for a delete-range operation.</param>
/// <param name="ScanQuery">Deeply cloned query, for a scan operation.</param>
public sealed record KvTestOperationRecord(
    long? TransactionId,
    KvTestOperation Operation,
    string Route,
    KvDurability? Durability = null,
    KvMode? Mode = null,
    IReadOnlyList<byte>? Key = null,
    IReadOnlyList<byte>? Value = null,
    IReadOnlyList<byte>? EndKey = null,
    KvScanQuery? ScanQuery = null);
