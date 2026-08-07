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
    public const uint RpcInvalidSubscriptionPattern = 6012;
    public const uint RpcSubscriptionLimit = 6013;
    public const uint ScheduleInvalidSubscriptionPattern = 7006;
    public const uint ScheduleSubscriptionLimit = 7007;
}
