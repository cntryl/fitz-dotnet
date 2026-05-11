namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamRecord
{
    public StreamRecord(ulong offset, byte[] body)
        : this(offset, null, null, body, null, 0)
    {
    }

    public StreamRecord(ulong offset, ulong? areaOffset, ulong? realmOffset, byte[] body, byte[]? metadata, ulong timestamp)
    {
        Offset = offset;
        AreaOffset = areaOffset;
        RealmOffset = realmOffset;
        Body = body;
        Metadata = metadata;
        Timestamp = timestamp;
    }

    public ulong Offset { get; init; }

    public ulong? AreaOffset { get; init; }

    public ulong? RealmOffset { get; init; }

    public byte[] Body { get; init; }

    public byte[]? Metadata { get; init; }

    public ulong Timestamp { get; init; }
}