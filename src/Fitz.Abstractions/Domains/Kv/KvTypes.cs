namespace Cntryl.Fitz.Abstractions.Domains.Kv;

/// <summary>
/// Transaction mode for KV begin operations.
/// </summary>
public enum KvMode : byte
{
    /// <summary>
    /// Read-only transaction mode.
    /// </summary>
    ReadOnly = 0,

    /// <summary>
    /// Read/write transaction mode.
    /// </summary>
    ReadWrite = 1,
}

/// <summary>
/// Durability mode for committed KV writes.
/// </summary>
public enum KvDurability : byte
{
    /// <summary>
    /// Buffered/async durability.
    /// </summary>
    Async = 0,

    /// <summary>
    /// Synchronous durability.
    /// </summary>
    Sync = 1,
}

/// <summary>
/// Result returned by KV get operations.
/// </summary>
/// <param name="Found">Whether the key was found.</param>
/// <param name="Value">Value bytes when found, otherwise <see langword="null"/>.</param>
public sealed record KvGetResult(bool Found, ReadOnlyMemory<byte>? Value = null);

/// <summary>
/// Key/value pair returned by KV scan operations.
/// </summary>
/// <param name="Key">Key bytes.</param>
/// <param name="Value">Value bytes.</param>
public sealed record KvPair(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value);

/// <summary>A page returned by KV SCAN.</summary>
public sealed record KvScanResult(IReadOnlyList<KvPair> Pairs, bool HasMore);

/// <summary>
/// Query parameters for KV scan operations.
/// </summary>
/// <param name="StartKey">Inclusive start key.</param>
/// <param name="EndKey">Exclusive end key.</param>
/// <param name="Limit">Maximum number of pairs to return.</param>
/// <param name="Reverse">Whether to scan in reverse order.</param>
public sealed record KvScanQuery(
    ReadOnlyMemory<byte>? StartKey = null,
    ReadOnlyMemory<byte>? EndKey = null,
    ulong? Limit = null,
    bool Reverse = false);

/// <summary>A committed KV mutation notification.</summary>
public sealed record KvNotification(string Route, ulong MutationCount);
