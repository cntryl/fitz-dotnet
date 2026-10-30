namespace Cntryl.Fitz;

/// <summary>
/// Authoritative broker domain error codes exposed by client APIs.
/// </summary>
/// <remarks>
/// Codes are assigned by the broker and grouped by domain in blocks of one thousand. Compare
/// a <c>Code</c> from a domain exception against these constants rather than matching on
/// message text. Use <c>Retryability</c> to decide whether an operation may be retried.
/// </remarks>
public static class FitzErrorCodes
{
    /// <summary>
    /// A KV transaction lost an isolation conflict and must be retried from the start.
    /// </summary>
    public const uint KvIsolationConflict = 1053;

    /// <summary>
    /// The KV backend was unavailable or failed to service the request.
    /// </summary>
    public const uint KvBackendError = 1006;

    /// <summary>The KV mailbox was full before the request was accepted.</summary>
    public const uint KvBusy = 1005;

    /// <summary>
    /// The KV subscription pattern was malformed or could not match three segments.
    /// </summary>
    public const uint KvInvalidSubscriptionPattern = 1003;

    /// <summary>
    /// The session reached the broker limit on wildcard KV registrations.
    /// </summary>
    public const uint KvSubscriptionLimit = 1004;

    /// <summary>
    /// An append operation found that the stream's committed position no longer matches its
    /// expected position.
    /// </summary>
    public const uint StreamConcurrencyConflict = 2050;

    /// <summary>
    /// An append session is already active for the resource stream.
    /// </summary>
    public const uint StreamSessionAlreadyActive = 2051;

    /// <summary>The Stream mailbox was full before the request was accepted.</summary>
    public const uint StreamBusy = 2005;

    /// <summary>
    /// The stream subscription selector was malformed or unsupported.
    /// </summary>
    public const uint StreamInvalidSubscriptionPattern = 2003;

    /// <summary>
    /// The session reached the broker limit on wildcard stream registrations.
    /// </summary>
    public const uint StreamSubscriptionLimit = 2004;

    /// <summary>
    /// The notice route or pattern was malformed.
    /// </summary>
    public const uint NoticeInvalidPattern = 3003;

    /// <summary>
    /// The session reached the broker limit on wildcard notice registrations.
    /// </summary>
    public const uint NoticeSubscriptionLimit = 3004;

    /// <summary>The Notice mailbox was full before the request was accepted.</summary>
    public const uint NoticeBusy = 3005;

    /// <summary>
    /// The queue subscription pattern was malformed or could not match three segments.
    /// </summary>
    public const uint QueueInvalidSubscriptionPattern = 4003;

    /// <summary>
    /// The session reached the broker limit on wildcard queue registrations.
    /// </summary>
    public const uint QueueSubscriptionLimit = 4004;

    /// <summary>
    /// The Queue broker declined the request because its mailbox was busy.
    /// </summary>
    public const uint QueueFull = 4005;

    /// <summary>
    /// The lease is currently held by another owner.
    /// </summary>
    public const uint LeaseHeld = 5050;

    /// <summary>
    /// The lease request was rejected as malformed by the broker.
    /// </summary>
    public const uint LeaseBadRequest = 5008;

    /// <summary>The Lease request queue was full before the request was accepted.</summary>
    public const uint LeaseQueueFull = 5056;

    /// <summary>
    /// The lease subscription route was not an exact <c>lease://realm/area/resource</c> route.
    /// </summary>
    public const uint LeaseInvalidSubscriptionRoute = 5002;

    /// <summary>
    /// The lease list continuation cursor was malformed or no longer valid.
    /// </summary>
    public const uint LeaseInvalidListCursor = 5057;

    /// <summary>
    /// The lease list pattern was malformed.
    /// </summary>
    public const uint LeaseInvalidListPattern = 5003;

    /// <summary>
    /// No worker produced a response within the request deadline.
    /// </summary>
    public const uint RpcTimeout = 6007;

    /// <summary>
    /// The assigned worker disconnected or unregistered before completion.
    /// </summary>
    public const uint RpcWorkerNotFound = 6050;

    /// <summary>
    /// The RPC pending queue was full and the request was not accepted.
    /// </summary>
    public const uint RpcBackpressure = 6005;

    /// <summary>
    /// The route exists but this session holds no registration for it.
    /// </summary>
    public const uint RpcRouteNotRegistered = 6051;

    /// <summary>
    /// The response referenced a correlation the broker no longer tracks.
    /// </summary>
    public const uint RpcCorrelationNotFound = 6052;

    /// <summary>
    /// A response frame arrived out of sequence within its stream.
    /// </summary>
    public const uint RpcInvalidSequence = 6053;

    /// <summary>
    /// A correlation identifier was reused while still in flight.
    /// </summary>
    public const uint RpcDuplicateCorrelation = 6054;

    /// <summary>
    /// The response came from a worker other than the one holding the invocation.
    /// </summary>
    public const uint RpcWrongWorker = 6055;

    /// <summary>
    /// The session is not permitted to invoke or serve the route.
    /// </summary>
    public const uint RpcUnauthorized = 6001;

    /// <summary>
    /// The worker or broker failed while servicing the invocation.
    /// </summary>
    public const uint RpcBackendError = 6006;

    /// <summary>
    /// The RPC route was malformed.
    /// </summary>
    public const uint RpcInvalidRoute = 6002;

    /// <summary>
    /// The RPC worker registration pattern was malformed.
    /// </summary>
    public const uint RpcInvalidSubscriptionPattern = 6003;

    /// <summary>
    /// The session reached the broker limit on wildcard worker registrations.
    /// </summary>
    public const uint RpcSubscriptionLimit = 6004;

    /// <summary>
    /// The schedule subscription pattern was malformed or could not match four segments.
    /// </summary>
    public const uint ScheduleInvalidSubscriptionPattern = 7003;

    /// <summary>
    /// The session reached the broker limit on wildcard schedule registrations.
    /// </summary>
    public const uint ScheduleSubscriptionLimit = 7004;

    /// <summary>
    /// Schedule storage or runtime failed after the request was accepted.
    /// </summary>
    public const uint ScheduleBackendError = 7006;

    /// <summary>The Schedule request was declined before application because the broker was busy.</summary>
    public const uint ScheduleBusy = 7005;
}
