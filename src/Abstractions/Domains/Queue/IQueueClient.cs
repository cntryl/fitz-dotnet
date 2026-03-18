namespace Cntryl.Fitz.Abstractions.Domains.Queue;

public interface IQueueClient
{
    Task<ulong> EnqueueAsync(
        string route,
        ReadOnlyMemory<byte> body,
        int? delayMs = null,
        CancellationToken ct = default
    );

    Task<IQueueReservedItem[]> ReserveAsync(
        string route,
        ulong leaseSeconds,
        int batchSize = 1,
        int? waitSeconds = null,
        CancellationToken ct = default
    );

    IAsyncEnumerable<QueueAvailabilityEvent> SubscribeAsync(
        string pattern,
        CancellationToken ct = default
    );
}

/// <summary>
/// Represents a reserved message from the queue lease operation.
/// </summary>
public interface IQueueReservedItem
{
    string Route { get; }
    ulong Id { get; }
    ulong Token { get; }
    ReadOnlyMemory<byte> Body { get; }
    uint Attempt { get; }

    Task ExtendAsync(ulong leaseSeconds, CancellationToken ct = default);
    Task CompleteAsync(CancellationToken ct = default);
    Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default);
}