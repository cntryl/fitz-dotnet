namespace Cntryl.Fitz.Protocol;

public static class MessageTypes
{
    public const ushort Connect = 1;

    public const ushort KvBegin = 100;
    public const ushort KvCommit = 101;
    public const ushort KvRollback = 102;
    public const ushort KvGet = 103;
    public const ushort KvPut = 104;
}