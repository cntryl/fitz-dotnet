using System.IO;
using Cntryl.Fitz.Abstractions;

namespace Cntryl.Fitz.Errors;

public static class Retryability
{
    public static bool IsRetryable(Exception? error)
    {
        return error switch
        {
            null => false,
            TimeoutException => true,
            RequestTimeoutException => true,
            ConnectionException => true,
            _ => error is IOException
                || IsRetryableDomainError(error)
                || IsTransientQueueCommitFailure(error),
        };
    }

    private static bool IsRetryableDomainError(Exception error)
    {
        return error switch
        {
            KvException kv when kv.DomainCode is FitzErrorCodes.KvIsolationConflict or FitzErrorCodes.KvBackendError => true,
            QueueException queue when queue.DomainCode == FitzErrorCodes.QueueFull => true,
            LeaseException lease when lease.DomainCode == FitzErrorCodes.LeaseHeld => true,
            RpcException rpc when IsRetryableRpcCode(rpc.Code) => true,
            _ => false,
        };
    }

    private static bool IsRetryableRpcCode(string code)
    {
        return code is "TIMEOUT" or "WORKER_NOT_FOUND" or "BACKPRESSURE" or "ROUTE_NOT_REGISTERED";
    }

    private static bool IsTransientQueueCommitFailure(Exception error)
    {
        if (error is not QueueException queue)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(queue.Message))
        {
            return false;
        }

        const StringComparison comparison = StringComparison.OrdinalIgnoreCase;
        if (!queue.Message.Contains("failed to commit transaction:", comparison))
        {
            return false;
        }

        return queue.Message.Contains("writestall(", comparison)
            || queue.Message.Contains("memory budget exceeded", comparison)
            || queue.Message.Contains("lease heartbeat reports unhealthy", comparison)
            || queue.Message.Contains("refusing writes", comparison);
    }

}
