namespace Cntryl.Fitz.Protocol;

public static class MessageTypes
{
    public const ushort Connect = 1;

    public const ushort KvBegin = 100;
    public const ushort KvCommit = 101;
    public const ushort KvRollback = 102;
    public const ushort KvGet = 103;
    public const ushort KvPut = 104;
    public const ushort KvInsert = 105;
    public const ushort KvDelete = 106;
    public const ushort KvDeleteRange = 107;
    public const ushort KvScan = 108;
    public const ushort KvSubscribe = 109;
    public const ushort KvUnsubscribe = 110;
    public const ushort KvNotify = 111;

    public const ushort QueueEnqueue = 200;
    public const ushort QueueReserve = 202;
    public const ushort QueueExtend = 203;
    public const ushort QueueComplete = 204;
    public const ushort QueueSubscribe = 207;
    public const ushort QueueUnsubscribe = 208;
    public const ushort QueueNotify = 209; // Server -> Client

    public const ushort RpcSubscribeWorker = 300;
    public const ushort RpcUnsubscribeWorker = 301;
    public const ushort RpcRequest = 302;
    public const ushort RpcResponse = 303;

    public const ushort LeaseAcquire = 400;
    public const ushort LeaseRenew = 401;
    public const ushort LeaseRelease = 402;
    public const ushort LeaseQuery = 403;
    public const ushort LeaseSubscribe = 407;
    public const ushort LeaseUnsubscribe = 408;
    public const ushort LeaseNotify = 409; // Server -> Client

    public const ushort NoticePublish = 500;
    public const ushort NoticeSubscribe = 501;
    public const ushort NoticeUnsubscribe = 502;
    public const ushort NoticeUnsubscribeAll = 503;
    public const ushort NoticeNotify = 504; // Server -> Client

    public const ushort StreamBegin = 600;
    public const ushort StreamAppend = 601;
    public const ushort StreamCommit = 602;
    public const ushort StreamRollback = 603;
    public const ushort StreamRead = 604;
    public const ushort StreamLast = 605;
    public const ushort StreamGetMetadata = 606;
    public const ushort StreamSubscribe = 607;
    public const ushort StreamUnsubscribe = 608;
    public const ushort StreamNotify = 609; // Server -> Client

    public const ushort ScheduleCreate = 700;
    public const ushort ScheduleCancel = 701;
    public const ushort ScheduleListPage = 702;
    public const ushort ScheduleSubscribe = 703;
    public const ushort ScheduleUnsubscribe = 704;
    public const ushort ScheduleNotify = 705; // Server -> Client
}
