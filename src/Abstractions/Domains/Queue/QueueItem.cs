namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public abstract class QueueItem : IQueueReservedItem
{
    protected QueueItem(string route, ReadOnlyMemory<byte> body, uint attempt = 1)
    {
        Route = route;
        Body = body;
        Attempt = attempt;
    }

    public string Route { get; }

    public ReadOnlyMemory<byte> Body { get; }

    public uint Attempt { get; }

    public abstract Task ExtendAsync(ulong leaseSeconds, CancellationToken ct = default);
    public abstract Task CompleteAsync(CancellationToken ct = default);
    public abstract Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default);
}
