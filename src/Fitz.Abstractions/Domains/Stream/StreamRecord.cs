namespace Cntryl.Fitz;

/// <summary>
/// A single committed stream record and its position at each scope.
/// </summary>
public sealed record StreamRecord
{
    /// <summary>
    /// Initializes a record with only its resource-scoped position.
    /// </summary>
    /// <param name="route">Concrete route the record belongs to.</param>
    /// <param name="offset">Resource-scoped sequence position.</param>
    /// <param name="body">Record payload.</param>
    public StreamRecord(string route, ulong offset, ReadOnlyMemory<byte> body)
        : this(route, offset, null, null, null, body, null, 0)
    {
    }

    /// <summary>
    /// Initializes a record with positions at every scope the broker reported.
    /// </summary>
    /// <param name="route">Concrete route the record belongs to.</param>
    /// <param name="offset">Resource-scoped sequence position.</param>
    /// <param name="areaOffset">Area-scoped position, when reported.</param>
    /// <param name="realmOffset">Realm-scoped position, when reported.</param>
    /// <param name="globalOffset">Global position, when reported.</param>
    /// <param name="body">Record payload.</param>
    /// <param name="metadata">Opaque metadata stored with the record.</param>
    /// <param name="timestamp">Broker commit timestamp.</param>
    public StreamRecord(string route, ulong offset, ulong? areaOffset, ulong? realmOffset, ulong? globalOffset, ReadOnlyMemory<byte> body, ReadOnlyMemory<byte>? metadata, ulong timestamp)
    {
        Route = route;
        Offset = offset;
        AreaOffset = areaOffset;
        RealmOffset = realmOffset;
        GlobalOffset = globalOffset;
        Body = body;
        Metadata = metadata;
        Timestamp = timestamp;
    }

    /// <summary>
    /// The concrete route this record belongs to. Never a request pattern, even when the
    /// read used a wildcard selector.
    /// </summary>
    public string Route { get; init; }

    /// <summary>Resource-scoped sequence position.</summary>
    public ulong Offset { get; init; }

    /// <summary>Area-scoped position, or <see langword="null"/> when the broker did not report one.</summary>
    public ulong? AreaOffset { get; init; }

    /// <summary>Realm-scoped position, or <see langword="null"/> when the broker did not report one.</summary>
    public ulong? RealmOffset { get; init; }

    /// <summary>Global position, or <see langword="null"/> when the broker did not report one.</summary>
    public ulong? GlobalOffset { get; init; }

    /// <summary>The record payload. The array is owned by the caller.</summary>
    public ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>Opaque metadata stored with the record, if any.</summary>
    public ReadOnlyMemory<byte>? Metadata { get; init; }

    /// <summary>Broker commit timestamp.</summary>
    public ulong Timestamp { get; init; }
}
