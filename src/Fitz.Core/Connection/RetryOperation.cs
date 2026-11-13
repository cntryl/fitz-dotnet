namespace Cntryl.Fitz.Connection;

enum RetryClass
{
    WaitOnly,
    ReplayableRead,
    ConfirmedNegativeRetry,
}

sealed record RetryOperation(
    string Domain,
    string Operation,
    RetryClass RetryClass
)
{
    /// <summary>Whether a failure of this operation may be retried automatically.</summary>
    /// <param name="exception">The failure.</param>
    /// <returns><see langword="true"/> when the operation is safe to repeat after this failure.</returns>
    internal bool ShouldRetry(Exception exception) => RetryClass switch
    {
        RetryClass.ReplayableRead => Retryability.IsRetryable(exception),
        RetryClass.ConfirmedNegativeRetry => exception is QueueException && Retryability.IsRetryable(exception),
        _ => false,
    };
}

/// <summary>
/// The fixed set of retryable operations. These are immutable and used on every request, so they
/// are shared rather than reallocated per call.
/// </summary>
static class RetryOperations
{
    internal static readonly RetryOperation KvGet = new("kv", "get", RetryClass.ReplayableRead);
    internal static readonly RetryOperation KvScan = new("kv", "scan", RetryClass.ReplayableRead);
    internal static readonly RetryOperation StreamRead = new("stream", "read", RetryClass.ReplayableRead);
    internal static readonly RetryOperation StreamLast = new("stream", "last", RetryClass.ReplayableRead);
    internal static readonly RetryOperation StreamMetadata = new("stream", "metadata", RetryClass.ReplayableRead);
    internal static readonly RetryOperation LeaseQuery = new("lease", "query", RetryClass.ReplayableRead);
}
