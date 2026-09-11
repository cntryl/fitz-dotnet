namespace Cntryl.Fitz.Abstractions;

/// <summary>Authoritative broker domain error codes exposed by client APIs.</summary>
public static class FitzErrorCodes
{
    public const uint KvIsolationConflict = 1004;
    public const uint KvBackendError = 1009;
    public const uint KvInvalidSubscriptionPattern = 1012;
    public const uint KvSubscriptionLimit = 1013;
    public const uint StreamInvalidSubscriptionPattern = 2010;
    public const uint StreamSubscriptionLimit = 2011;
    public const uint NoticeInvalidPattern = 3002;
    public const uint NoticeSubscriptionLimit = 3003;
    public const uint QueueInvalidSubscriptionPattern = 4010;
    public const uint QueueSubscriptionLimit = 4011;
    public const uint QueueFull = 4005;
    public const uint LeaseHeld = 5001;
    public const uint LeaseBadRequest = 5008;
    public const uint LeaseInvalidSubscriptionRoute = 5010;
    public const uint LeaseInvalidListCursor = 5011;
    public const uint LeaseInvalidListPattern = 5012;
    public const uint RpcTimeout = 6001;
    public const uint RpcWorkerNotFound = 6002;
    public const uint RpcBackpressure = 6003;
    public const uint RpcRouteNotRegistered = 6004;
    public const uint RpcCorrelationNotFound = 6005;
    public const uint RpcInvalidSequence = 6006;
    public const uint RpcDuplicateCorrelation = 6007;
    public const uint RpcWrongWorker = 6008;
    public const uint RpcUnauthorized = 6009;
    public const uint RpcBackendError = 6010;
    public const uint RpcInvalidRoute = 6011;
    public const uint RpcInvalidSubscriptionPattern = 6012;
    public const uint RpcSubscriptionLimit = 6013;
    public const uint ScheduleInvalidSubscriptionPattern = 7006;
    public const uint ScheduleSubscriptionLimit = 7007;
    public const uint ScheduleBackendError = 7010;
}
