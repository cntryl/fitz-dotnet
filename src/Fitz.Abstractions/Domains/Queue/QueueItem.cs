namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public abstract class QueueItem : IQueueReservedItem
{
    /// <summary>Sentinel used when the current wire protocol does not provide an attempt count.</summary>
    public const uint AttemptUnavailable = 0;

    protected QueueItem(string route, ReadOnlyMemory<byte> body, uint attempt = AttemptUnavailable)
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
