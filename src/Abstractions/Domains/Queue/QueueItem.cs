namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public abstract record QueueItem(string Route, ulong Id, ulong Token, ReadOnlyMemory<byte> Body, uint Attempt = 1)
    : IQueueReservedItem
{
    public abstract Task ExtendAsync(ulong leaseSeconds, CancellationToken ct = default);
    public abstract Task CompleteAsync(CancellationToken ct = default);
    public abstract Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default);
}