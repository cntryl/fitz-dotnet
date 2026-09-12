namespace Cntryl.Fitz.Protocol;

/// <summary>
/// Fitz wire message type identifiers. Each constant is the <c>u16</c> opcode carried in a
/// transport frame header, and its numeric value is fixed by the protocol.
/// </summary>
/// <remarks>
/// Opcodes are grouped by domain in blocks of one hundred. Opcodes documented as
/// broker-originated arrive unsolicited and are dispatched to subscription handlers rather
/// than matched to a pending request.
/// </remarks>
static class MessageTypes
{
    /// <summary>
    /// Opens a session and presents credentials when the broker requires authentication.
    /// </summary>
    public const ushort Connect = 1;

    /// <summary>
    /// Labels the request record that immediately follows it in the same transport frame with a
    /// client-generated <c>u64</c> identifier.
    /// </summary>
    public const ushort Correlate = 2;

    /// <summary>
    /// Echoes a <see cref="Correlate"/> identifier ahead of the response it belongs to, in the same
    /// transport frame. The broker sends it only for requests that carried <see cref="Correlate"/>.
    /// </summary>
    public const ushort Correlated = 3;

    /// <summary>
    /// Unsolicited broker capability advertisement, sent once per session on connect.
    /// </summary>
    public const ushort ServerHello = 4;

    /// <summary>
    /// Opens a KV transaction over a route and returns its transaction identifier.
    /// </summary>
    public const ushort KvBegin = 100;

    /// <summary>
    /// Commits an open KV transaction, making its writes durable per the negotiated durability.
    /// </summary>
    public const ushort KvCommit = 101;

    /// <summary>
    /// Abandons an open KV transaction and discards its buffered writes.
    /// </summary>
    public const ushort KvRollback = 102;

    /// <summary>
    /// Reads the value for a key within an open transaction.
    /// </summary>
    public const ushort KvGet = 103;

    /// <summary>
    /// Writes a key/value pair, overwriting any existing value.
    /// </summary>
    public const ushort KvPut = 104;

    /// <summary>
    /// Writes a key/value pair only when the key does not already exist.
    /// </summary>
    public const ushort KvInsert = 105;

    /// <summary>
    /// Removes a single key.
    /// </summary>
    public const ushort KvDelete = 106;

    /// <summary>
    /// Removes every key within an inclusive range.
    /// </summary>
    public const ushort KvDeleteRange = 107;

    /// <summary>
    /// Enumerates key/value pairs within a range.
    /// </summary>
    public const ushort KvScan = 108;

    /// <summary>
    /// Registers interest in key changes matching a route or pattern.
    /// </summary>
    public const ushort KvSubscribe = 109;

    /// <summary>
    /// Cancels a KV subscription.
    /// </summary>
    public const ushort KvUnsubscribe = 110;

    /// <summary>
    /// Broker-originated notification carrying a KV change event.
    /// </summary>
    public const ushort KvNotify = 111;

    /// <summary>
    /// Appends a message to a queue, optionally with a whole-second delivery delay.
    /// </summary>
    public const ushort QueueEnqueue = 200;

    /// <summary>
    /// Leases one or more messages for processing, with a visibility timeout.
    /// </summary>
    public const ushort QueueReserve = 202;

    /// <summary>
    /// Extends the visibility timeout of a reserved message.
    /// </summary>
    public const ushort QueueExtend = 203;

    /// <summary>
    /// Acknowledges a reserved message, removing it from the queue permanently.
    /// </summary>
    public const ushort QueueComplete = 204;

    /// <summary>
    /// Registers interest in availability changes for queues matching a pattern.
    /// </summary>
    public const ushort QueueSubscribe = 207;

    /// <summary>
    /// Cancels a queue subscription.
    /// </summary>
    public const ushort QueueUnsubscribe = 208;

    /// <summary>
    /// Broker-originated notification carrying queue availability counts.
    /// </summary>
    public const ushort QueueNotify = 209;

    /// <summary>
    /// Registers this session as a worker for routes matching a pattern.
    /// </summary>
    public const ushort RpcSubscribeWorker = 300;

    /// <summary>
    /// Withdraws a worker registration.
    /// </summary>
    public const ushort RpcUnsubscribeWorker = 301;

    /// <summary>
    /// Invokes a route; also delivers an inbound invocation to a registered worker.
    /// </summary>
    public const ushort RpcRequest = 302;

    /// <summary>
    /// Carries one frame of a response, including the terminal frame of a stream.
    /// </summary>
    public const ushort RpcResponse = 303;

    /// <summary>
    /// Claims a lease, returning its fencing token and expiry.
    /// </summary>
    public const ushort LeaseAcquire = 400;

    /// <summary>
    /// Extends a held lease before it expires.
    /// </summary>
    public const ushort LeaseRenew = 401;

    /// <summary>
    /// Releases a held lease so another holder may claim it.
    /// </summary>
    public const ushort LeaseRelease = 402;

    /// <summary>
    /// Reads the current holder and expiry of a lease without claiming it.
    /// </summary>
    public const ushort LeaseQuery = 403;

    /// <summary>
    /// Registers interest in ownership changes for an exact lease route.
    /// </summary>
    public const ushort LeaseSubscribe = 407;

    /// <summary>
    /// Cancels a lease subscription.
    /// </summary>
    public const ushort LeaseUnsubscribe = 408;

    /// <summary>
    /// Broker-originated notification carrying a lease ownership change.
    /// </summary>
    public const ushort LeaseNotify = 409;

    /// <summary>
    /// Enumerates leases matching a pattern, one page per request.
    /// </summary>
    public const ushort LeaseList = 410;

    /// <summary>
    /// Publishes a fire-and-forget notice to a route.
    /// </summary>
    public const ushort NoticePublish = 500;

    /// <summary>
    /// Registers interest in notices matching a route or pattern.
    /// </summary>
    public const ushort NoticeSubscribe = 501;

    /// <summary>
    /// Cancels a single notice subscription.
    /// </summary>
    public const ushort NoticeUnsubscribe = 502;

    /// <summary>
    /// Cancels every notice subscription held by this session.
    /// </summary>
    public const ushort NoticeUnsubscribeAll = 503;

    /// <summary>
    /// Broker-originated notification carrying a published notice.
    /// </summary>
    public const ushort NoticeNotify = 504;

    /// <summary>
    /// Opens a stream append session and returns its session identifier.
    /// </summary>
    public const ushort StreamBegin = 600;

    /// <summary>
    /// Appends a record to an open stream session.
    /// </summary>
    public const ushort StreamAppend = 601;

    /// <summary>
    /// Commits an open stream session, publishing its appended records.
    /// </summary>
    public const ushort StreamCommit = 602;

    /// <summary>
    /// Abandons an open stream session and discards its appended records.
    /// </summary>
    public const ushort StreamRollback = 603;

    /// <summary>
    /// Reads a page of records from a sequence position, with optional filtering.
    /// </summary>
    public const ushort StreamRead = 604;

    /// <summary>
    /// Reads the most recent record for a concrete stream route.
    /// </summary>
    public const ushort StreamLast = 605;

    /// <summary>
    /// Reads stream metadata, including the current sequence position.
    /// </summary>
    public const ushort StreamGetMetadata = 606;

    /// <summary>
    /// Registers interest in commits matching a route or selector.
    /// </summary>
    public const ushort StreamSubscribe = 607;

    /// <summary>
    /// Cancels a stream subscription.
    /// </summary>
    public const ushort StreamUnsubscribe = 608;

    /// <summary>
    /// Broker-originated notification carrying a stream commit event.
    /// </summary>
    public const ushort StreamNotify = 609;

    /// <summary>
    /// Registers a cron schedule that delivers a payload to a route.
    /// </summary>
    public const ushort ScheduleCreate = 700;

    /// <summary>
    /// Removes a registered schedule.
    /// </summary>
    public const ushort ScheduleCancel = 701;

    /// <summary>
    /// Reads a page of registered schedules plus the total count.
    /// </summary>
    public const ushort ScheduleListPage = 702;

    /// <summary>
    /// Registers interest in schedule firings matching a pattern.
    /// </summary>
    public const ushort ScheduleSubscribe = 703;

    /// <summary>
    /// Cancels a schedule subscription.
    /// </summary>
    public const ushort ScheduleUnsubscribe = 704;

    /// <summary>
    /// Broker-originated notification carrying a schedule firing.
    /// </summary>
    public const ushort ScheduleNotify = 705;
}
