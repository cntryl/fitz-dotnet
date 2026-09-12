
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
        Assert.True(Retryability.IsRetryable(new QueueException("queue full", "ENQUEUE_FAILED", 1, FitzErrorCodes.QueueFull)));
        Assert.True(Retryability.IsRetryable(new LeaseException("lease held", "LEASE_HELD", 1, FitzErrorCodes.LeaseHeld)));
        Assert.True(Retryability.IsRetryable(new KvException("conflict", "PUT_FAILED", 1, FitzErrorCodes.KvIsolationConflict)));
        Assert.True(Retryability.IsRetryable(new ScheduleException("backend busy", "BACKEND_ERROR", 1, FitzErrorCodes.ScheduleBackendError)));
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
        Assert.False(Retryability.IsRetryable(new QueueException(
            "failed to commit transaction: memory budget exceeded",
            "ENQUEUE_FAILED",
            1)));
    }

    [Fact]
    public void ShouldClassifyRetryableErrorGivenRpcTimeoutWhenRetryPolicyEvaluated() => Assert.True(Retryability.IsRetryable(new RpcException("timed out", "TIMEOUT")));

    [Fact]
    public void ShouldClassifyRetryableErrorGivenRpcNoWorkerWhenRetryPolicyEvaluated() => Assert.True(Retryability.IsRetryable(new RpcException("no worker", "WORKER_NOT_FOUND")));

    [Fact]
    public void ShouldFailFastGivenRpcUnauthorizedWhenRetryPolicyEvaluated() => Assert.False(Retryability.IsRetryable(new RpcException("unauthorized", "UNAUTHORIZED")));
}
