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
    Buffered = 0,

    /// <summary>
    /// Synchronous durability.
    /// </summary>
    Sync = 1,
}

/// <summary>
/// Result of a KV get: the value that was found (<see cref="KvFound"/>), or
/// <see cref="KvNotFound"/> when the key holds no value.
/// </summary>
/// <remarks>
/// <para>
/// Match on the case to read the result. A <c>switch</c> over both cases is checked for
/// completeness by the compiler:
/// </para>
/// <code>
/// if (await transaction.GetAsync(key) is KvFound { Value: var bytes })
/// {
///     Use(bytes);
/// }
///
/// var text = await transaction.GetAsync(key) switch
/// {
///     KvFound found => Decode(found.Value),
///     KvNotFound => "(none)",
/// };
/// </code>
/// <para>
/// Prefer those forms to <c>result is KvFound found</c>: code analysis rule CA1508 does not
/// understand union patterns yet and reports that form as always true.
/// </para>
/// </remarks>
public union KvGetResult(KvFound, KvNotFound) : IEquatable<KvGetResult>
{
    /// <summary>Whether two results hold equal cases.</summary>
    /// <param name="other">The result to compare with.</param>
    /// <returns><see langword="true"/> when both hold the same case with equal contents.</returns>
    public bool Equals(KvGetResult other) => Equals(Value, other.Value);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is KvGetResult other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => Value?.GetHashCode() ?? 0;

    /// <summary>Whether two results hold equal cases.</summary>
    public static bool operator ==(KvGetResult left, KvGetResult right) => left.Equals(right);

    /// <summary>Whether two results hold different cases or contents.</summary>
    public static bool operator !=(KvGetResult left, KvGetResult right) => !left.Equals(right);
}

/// <summary>
/// The value a KV get found at its key.
/// </summary>
/// <param name="Value">Value bytes. Empty when the key holds an empty value.</param>
public sealed record KvFound(ReadOnlyMemory<byte> Value);

/// <summary>
/// The outcome of a KV get for a key that holds no value.
/// </summary>
public sealed record KvNotFound
{
    KvNotFound()
    {
    }

    /// <summary>The shared instance; every "not found" result is this object.</summary>
    public static KvNotFound Instance { get; } = new();
}

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
