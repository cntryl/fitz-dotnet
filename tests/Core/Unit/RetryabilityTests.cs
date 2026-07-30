using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RetryabilityTests
{
    [Fact]
    public void should_classify_transient_errors_as_retryable()
    {
        Assert.True(Retryability.IsRetryable(new TimeoutException("timed out")));
        Assert.True(Retryability.IsRetryable(new RequestTimeoutException("timed out")));
        Assert.True(Retryability.IsRetryable(new ConnectionException("connection closed")));
        Assert.True(Retryability.IsRetryable(new QueueException("queue full", "ENQUEUE_FAILED", 4)));
        Assert.True(Retryability.IsRetryable(new LeaseException("lease held", "LEASE_HELD", 1)));
        Assert.True(Retryability.IsRetryable(new KvException("conflict", "PUT_FAILED", 3)));
    }

    [Fact]
    public void should_not_classify_other_errors_as_retryable()
    {
        Assert.False(Retryability.IsRetryable(new InvalidOperationException("boom")));
        Assert.False(Retryability.IsRetryable(new AuthenticationException("unauthorized")));
        Assert.False(Retryability.IsRetryable(new QueueException("invalid token", "INVALID_TOKEN", 3)));
        Assert.False(Retryability.IsRetryable(new LeaseException("not found", "LEASE_NOT_FOUND", 2)));
        Assert.False(Retryability.IsRetryable(new StreamException("missing", "STREAM_NOT_FOUND", 1)));
    }

    [Fact]
    public void should_classify_retryable_error_given_rpc_timeout_when_retry_policy_evaluated()
    {
        Assert.True(Retryability.IsRetryable(new RpcException("timed out", "TIMEOUT")));
    }

    [Fact]
    public void should_classify_retryable_error_given_rpc_no_worker_when_retry_policy_evaluated()
    {
        Assert.True(Retryability.IsRetryable(new RpcException("no worker", "WORKER_NOT_FOUND")));
    }

    [Fact]
    public void should_fail_fast_given_rpc_unauthorized_when_retry_policy_evaluated()
    {
        Assert.False(Retryability.IsRetryable(new RpcException("unauthorized", "UNAUTHORIZED")));
    }
}
