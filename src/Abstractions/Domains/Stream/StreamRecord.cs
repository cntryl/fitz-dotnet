namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamRecord
{
    public StreamRecord(string route, ulong offset, byte[] body)
        : this(route, offset, null, null, null, body, null, 0)
    {
    }

    public StreamRecord(string route, ulong offset, ulong? areaOffset, ulong? realmOffset, ulong? globalOffset, byte[] body, byte[]? metadata, ulong timestamp)
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

    public string Route { get; init; }

    public ulong Offset { get; init; }

    public ulong? AreaOffset { get; init; }

    public ulong? RealmOffset { get; init; }

    public ulong? GlobalOffset { get; init; }

    public byte[] Body { get; init; }

    public byte[]? Metadata { get; init; }

    public ulong Timestamp { get; init; }
}
