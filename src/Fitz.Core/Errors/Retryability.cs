using System.IO;

namespace Cntryl.Fitz;

/// <summary>
/// Classifies whether a failed operation may be retried.
/// </summary>
/// <remarks>
/// Retryability is about the error, not about safety. A retryable classification still
/// requires the operation itself to be safe to repeat: KV commit, stream commit, queue
/// completion, and lease release become terminal only after broker success, and a definite
/// rejection leaves the handle retryable.
/// </remarks>
public static class Retryability
{
    /// <summary>
    /// Whether an operation that failed with this error may be retried.
    /// </summary>
    /// <param name="error">The failure to classify. <see langword="null"/> is not retryable.</param>
    /// <returns><see langword="true"/> when a retry is worthwhile.</returns>
    public static bool IsRetryable(Exception? error)
    {
        return error switch
        {
            null => false,
            TimeoutException => true,
            RequestTimeoutException => true,
            ConnectionException => true,
            _ => error is IOException
                || IsRetryableDomainError(error),
        };
    }

    static bool IsRetryableDomainError(Exception error)
    {
        return error switch
        {
            KvException kv when kv.DomainCode is FitzErrorCodes.KvIsolationConflict or FitzErrorCodes.KvBackendError => true,
            QueueException queue when queue.DomainCode == FitzErrorCodes.QueueFull => true,
            LeaseException lease when lease.DomainCode == FitzErrorCodes.LeaseHeld => true,
            RpcException rpc when rpc.DomainCode is FitzErrorCodes.RpcTimeout or FitzErrorCodes.RpcWorkerNotFound or FitzErrorCodes.RpcBackpressure or FitzErrorCodes.RpcRouteNotRegistered
                || rpc.Code is "TIMEOUT" or "WORKER_NOT_FOUND" or "BACKPRESSURE" or "ROUTE_NOT_REGISTERED" => true,
            ScheduleException schedule when schedule.DomainCode == FitzErrorCodes.ScheduleBackendError => true,
            _ => false,
        };
    }

}
