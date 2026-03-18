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

public sealed record KvGetResult(bool Found, byte[]? Value = null);