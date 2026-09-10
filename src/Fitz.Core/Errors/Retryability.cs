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
            RpcException rpc when IsRetryableRpcCode(rpc.Code) => true,
            ScheduleException schedule when schedule.DomainCode == FitzErrorCodes.ScheduleBackendError => true,
            _ => false,
        };
    }

    static bool IsRetryableRpcCode(string code) => code is "TIMEOUT" or "WORKER_NOT_FOUND" or "BACKPRESSURE" or "ROUTE_NOT_REGISTERED";

}
