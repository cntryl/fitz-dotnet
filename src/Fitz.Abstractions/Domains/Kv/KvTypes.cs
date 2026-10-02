namespace Cntryl.Fitz;

/// <summary>
/// Transaction mode for KV begin operations.
/// </summary>
public enum KvMode
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
public enum KvDurability
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
/// <param name="StartKey">Inclusive directional bound: lower forward, upper in reverse scans.</param>
/// <param name="EndKey">Exclusive directional bound: upper forward, lower in reverse scans.</param>
/// <param name="Limit">Maximum pairs to return; zero uses the broker's default page budget.</param>
/// <param name="Reverse">Whether to scan in reverse order.</param>
/// <param name="StartExclusive">Whether to resume strictly after the directional start key.</param>
public sealed record KvScanQuery(
    ReadOnlyMemory<byte>? StartKey = null,
    ReadOnlyMemory<byte>? EndKey = null,
    uint? Limit = null,
    bool Reverse = false,
    bool StartExclusive = false);

/// <summary>A committed KV mutation notification.</summary>
public sealed record KvNotification(string Route, ulong MutationCount);
