
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
        Assert.True(Retryability.IsRetryable(WithWire(new QueueException("queue full", "ENQUEUE_FAILED", 1, FitzErrorCodes.QueueFull))));
        Assert.True(Retryability.IsRetryable(WithWire(new LeaseException("lease held", "LEASE_HELD", 1, FitzErrorCodes.LeaseHeld))));
        Assert.True(Retryability.IsRetryable(WithWire(new KvException("conflict", "PUT_FAILED", 1, FitzErrorCodes.KvIsolationConflict))));
        Assert.True(Retryability.IsRetryable(WithWire(new KvException("busy", "BUSY", 1, FitzErrorCodes.KvBusy))));
        Assert.True(Retryability.IsRetryable(WithWire(new StreamException("busy", "BUSY", 1, FitzErrorCodes.StreamBusy))));
        Assert.True(Retryability.IsRetryable(WithWire(new NoticeException("busy", "BUSY", 1, FitzErrorCodes.NoticeBusy))));
        Assert.True(Retryability.IsRetryable(WithWire(new LeaseException("queue full", "QUEUE_FULL", 1, FitzErrorCodes.LeaseQueueFull))));
        Assert.False(Retryability.IsRetryable(WithWire(new ScheduleException("backend busy", "BACKEND_ERROR", 1, FitzErrorCodes.ScheduleBackendError), 6, 1)));
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
    public void ShouldClassifyIndeterminateErrorGivenRpcTimeoutWhenRetryPolicyEvaluated() => Assert.False(Retryability.IsRetryable(WithWire(new RpcException("timed out", "TIMEOUT", 1, FitzErrorCodes.RpcTimeout), 7, 1)));

    [Fact]
    public void ShouldClassifyRetryableErrorGivenRpcNoWorkerWhenRetryPolicyEvaluated() => Assert.True(Retryability.IsRetryable(WithWire(new RpcException("no worker", "ROUTE_NOT_REGISTERED", 1, FitzErrorCodes.RpcRouteNotRegistered), 0)));

    [Fact]
    public void ShouldFailFastGivenRpcUnauthorizedWhenRetryPolicyEvaluated() => Assert.False(Retryability.IsRetryable(new RpcException("unauthorized", "UNAUTHORIZED")));
    static T WithWire<T>(T error, byte category = 5, byte effect = 0) where T : FitzException
    {
        error.SetWireError(category, effect);
        return error;
    }
}
