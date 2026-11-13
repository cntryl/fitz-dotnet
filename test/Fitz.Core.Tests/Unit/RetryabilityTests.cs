
namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RetryabilityTests
{
    [Fact]
    public void ShouldClassifyErrorsAsRetryableGivenTransientCodesWhenPolicyEvaluated()
    {
        // Arrange
        // Act
        // Assert
        Assert.True(Retryability.IsRetryable(new TimeoutException("timed out")));
        Assert.True(Retryability.IsRetryable(new RequestTimeoutException("timed out")));
        Assert.True(Retryability.IsRetryable(new ConnectionException("connection closed")));
        Assert.True(Retryability.IsRetryable(new QueueException("queue full", "ENQUEUE_FAILED", 1, FitzErrorCodes.QueueFull) { ErrorClass = 5, ErrorEffect = 0 }));
        Assert.True(Retryability.IsRetryable(new LeaseException("lease held", "LEASE_HELD", 1, FitzErrorCodes.LeaseHeld) { ErrorClass = 5, ErrorEffect = 0 }));
        Assert.True(Retryability.IsRetryable(new KvException("conflict", "PUT_FAILED", 1, FitzErrorCodes.KvIsolationConflict) { ErrorClass = 5, ErrorEffect = 0 }));
        Assert.True(Retryability.IsRetryable(new KvException("busy", "BUSY", 1, FitzErrorCodes.KvBusy) { ErrorClass = 5, ErrorEffect = 0 }));
        Assert.True(Retryability.IsRetryable(new StreamException("busy", "BUSY", 1, FitzErrorCodes.StreamBusy) { ErrorClass = 5, ErrorEffect = 0 }));
        Assert.True(Retryability.IsRetryable(new NoticeException("busy", "BUSY", 1, FitzErrorCodes.NoticeBusy) { ErrorClass = 5, ErrorEffect = 0 }));
        Assert.True(Retryability.IsRetryable(new LeaseException("queue full", "QUEUE_FULL", 1, FitzErrorCodes.LeaseQueueFull) { ErrorClass = 5, ErrorEffect = 0 }));
        Assert.False(Retryability.IsRetryable(new ScheduleException("backend busy", "BACKEND_ERROR", 1, FitzErrorCodes.ScheduleBackendError) { ErrorClass = 6, ErrorEffect = 1 }));
    }

    [Fact]
    public void ShouldClassifyErrorsAsNonretryableGivenFatalCodesWhenPolicyEvaluated()
    {
        // Arrange
        // Act
        // Assert
        Assert.False(Retryability.IsRetryable(new InvalidOperationException("boom")));
        Assert.False(Retryability.IsRetryable(new AuthenticationException("unauthorized")));
        Assert.False(Retryability.IsRetryable(new QueueException("invalid token", "INVALID_TOKEN", 1, 4001)));
        Assert.False(Retryability.IsRetryable(new LeaseException("not found", "LEASE_NOT_FOUND", 1, 5004)));
        Assert.False(Retryability.IsRetryable(new StreamException("missing", "STREAM_NOT_FOUND", 1)));
        Assert.False(Retryability.IsRetryable(new KvException("backend", "BACKEND_ERROR", 1, FitzErrorCodes.KvBackendError)));
        Assert.False(Retryability.IsRetryable(new QueueException(
            "failed to commit transaction: memory budget exceeded",
            "ENQUEUE_FAILED",
            1)));
    }

    [Fact]
    public void ShouldClassifyIndeterminateErrorGivenRpcTimeoutWhenRetryPolicyEvaluated() => Assert.False(Retryability.IsRetryable(new RpcException("timed out", "TIMEOUT", 1, FitzErrorCodes.RpcTimeout) { ErrorClass = 7, ErrorEffect = 1 }));

    [Fact]
    public void ShouldClassifyRetryableErrorGivenRpcNoWorkerWhenRetryPolicyEvaluated() => Assert.True(Retryability.IsRetryable(new RpcException("no worker", "ROUTE_NOT_REGISTERED", 1, FitzErrorCodes.RpcRouteNotRegistered) { ErrorClass = 0, ErrorEffect = 0 }));

    [Fact]
    public void ShouldFailFastGivenRpcUnauthorizedWhenRetryPolicyEvaluated() => Assert.False(Retryability.IsRetryable(new RpcException("unauthorized", "UNAUTHORIZED")));
}
