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
);

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
