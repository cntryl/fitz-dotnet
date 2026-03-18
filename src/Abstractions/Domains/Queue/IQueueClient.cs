namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public interface IQueueClient
{
    Task<ulong> EnqueueAsync(
        string route,
        byte[] body,
        int? delayMs = null,
        CancellationToken cancellationToken = default
    );
}