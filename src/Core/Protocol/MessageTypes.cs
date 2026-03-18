namespace Cntryl.Fitz.Protocol;

public static class MessageTypes
{
    public const ushort Connect = 1;

    public const ushort KvBegin = 100;
    public const ushort KvCommit = 101;
    public const ushort KvRollback = 102;
    public const ushort KvGet = 103;
    public const ushort KvPut = 104;

    public const ushort QueueEnqueue = 200;

    public const ushort RpcRequest = 302;

    public const ushort LeaseAcquire = 400;
    public const ushort LeaseRenew = 401;
    public const ushort LeaseRelease = 402;
    public const ushort LeaseQuery = 403;

    public const ushort NoticePublish = 500;

    public const ushort StreamBegin = 600;
    public const ushort StreamAppend = 601;
    public const ushort StreamCommit = 602;
    public const ushort StreamRollback = 603;
    public const ushort StreamRead = 604;
    public const ushort StreamGetMetadata = 606;

    public const ushort ScheduleCreate = 700;
    public const ushort ScheduleCancel = 701;
}