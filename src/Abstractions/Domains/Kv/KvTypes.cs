namespace Cntryl.Fitz.Abstractions.Domains.Kv;

public enum KvMode : byte
{
    ReadOnly = 0,
    ReadWrite = 1,
}

public enum KvDurability : byte
{
    Async = 0,
    Sync = 1,
}

public sealed record KvGetResult(bool Found, ReadOnlyMemory<byte>? Value = null);

public sealed record KvPair(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value);

public sealed record KvScanQuery(
    ReadOnlyMemory<byte>? StartKey = null,
    ReadOnlyMemory<byte>? EndKey = null,
    ulong? Limit = null,
    bool Reverse = false);