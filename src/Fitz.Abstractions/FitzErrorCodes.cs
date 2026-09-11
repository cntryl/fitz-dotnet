namespace Cntryl.Fitz.Abstractions;

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
    public const uint KvIsolationConflict = 1004;

    /// <summary>
    /// The KV backend was unavailable or failed to service the request.
    /// </summary>
    public const uint KvBackendError = 1009;

    /// <summary>
    /// The KV subscription pattern was malformed or could not match three segments.
    /// </summary>
    public const uint KvInvalidSubscriptionPattern = 1012;

    /// <summary>
    /// The session reached the broker limit on wildcard KV registrations.
    /// </summary>
    public const uint KvSubscriptionLimit = 1013;

    /// <summary>
    /// The stream subscription selector was malformed or unsupported.
    /// </summary>
    public const uint StreamInvalidSubscriptionPattern = 2010;

    /// <summary>
    /// The session reached the broker limit on wildcard stream registrations.
    /// </summary>
    public const uint StreamSubscriptionLimit = 2011;

    /// <summary>
    /// The notice route or pattern was malformed.
    /// </summary>
    public const uint NoticeInvalidPattern = 3002;

    /// <summary>
    /// The session reached the broker limit on wildcard notice registrations.
    /// </summary>
    public const uint NoticeSubscriptionLimit = 3003;

    /// <summary>
    /// The queue subscription pattern was malformed or could not match three segments.
    /// </summary>
    public const uint QueueInvalidSubscriptionPattern = 4010;

    /// <summary>
    /// The session reached the broker limit on wildcard queue registrations.
    /// </summary>
    public const uint QueueSubscriptionLimit = 4011;

    /// <summary>
    /// The queue reached its configured depth and rejected the enqueue.
    /// </summary>
    public const uint QueueFull = 4005;

    /// <summary>
    /// The lease is currently held by another owner.
    /// </summary>
    public const uint LeaseHeld = 5001;

    /// <summary>
    /// The lease request was rejected as malformed by the broker.
    /// </summary>
    public const uint LeaseBadRequest = 5008;

    /// <summary>
    /// The lease subscription route was not an exact <c>lease://realm/area/resource</c> route.
    /// </summary>
    public const uint LeaseInvalidSubscriptionRoute = 5010;

    /// <summary>
    /// The lease list continuation cursor was malformed or no longer valid.
    /// </summary>
    public const uint LeaseInvalidListCursor = 5011;

    /// <summary>
    /// The lease list pattern was malformed.
    /// </summary>
    public const uint LeaseInvalidListPattern = 5012;

    /// <summary>
    /// No worker produced a response within the request deadline.
    /// </summary>
    public const uint RpcTimeout = 6001;

    /// <summary>
    /// No worker is registered for the requested route.
    /// </summary>
    public const uint RpcWorkerNotFound = 6002;

    /// <summary>
    /// The target worker is saturated and shed the request.
    /// </summary>
    public const uint RpcBackpressure = 6003;

    /// <summary>
    /// The route exists but this session holds no registration for it.
    /// </summary>
    public const uint RpcRouteNotRegistered = 6004;

    /// <summary>
    /// The response referenced a correlation the broker no longer tracks.
    /// </summary>
    public const uint RpcCorrelationNotFound = 6005;

    /// <summary>
    /// A response frame arrived out of sequence within its stream.
    /// </summary>
    public const uint RpcInvalidSequence = 6006;

    /// <summary>
    /// A correlation identifier was reused while still in flight.
    /// </summary>
    public const uint RpcDuplicateCorrelation = 6007;

    /// <summary>
    /// The response came from a worker other than the one holding the invocation.
    /// </summary>
    public const uint RpcWrongWorker = 6008;

    /// <summary>
    /// The session is not permitted to invoke or serve the route.
    /// </summary>
    public const uint RpcUnauthorized = 6009;

    /// <summary>
    /// The worker or broker failed while servicing the invocation.
    /// </summary>
    public const uint RpcBackendError = 6010;

    /// <summary>
    /// The RPC route was malformed.
    /// </summary>
    public const uint RpcInvalidRoute = 6011;

    /// <summary>
    /// The RPC worker registration pattern was malformed.
    /// </summary>
    public const uint RpcInvalidSubscriptionPattern = 6012;

    /// <summary>
    /// The session reached the broker limit on wildcard worker registrations.
    /// </summary>
    public const uint RpcSubscriptionLimit = 6013;

    /// <summary>
    /// The schedule subscription pattern was malformed or could not match four segments.
    /// </summary>
    public const uint ScheduleInvalidSubscriptionPattern = 7006;

    /// <summary>
    /// The session reached the broker limit on wildcard schedule registrations.
    /// </summary>
    public const uint ScheduleSubscriptionLimit = 7007;

    /// <summary>
    /// The schedule backend was unavailable or saturated; distinct from malformed cron input.
    /// </summary>
    public const uint ScheduleBackendError = 7010;
}
