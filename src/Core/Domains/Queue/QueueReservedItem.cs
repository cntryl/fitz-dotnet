using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Queue;

/// <summary>
/// Concrete implementation of a reserved queue item.
/// Provides methods to extend the lease, complete the item, or acknowledge with explicit token.
/// </summary>
internal sealed record QueueReservedItem(
    string Route, 
    ulong Id, 
    ulong Token, 
    ReadOnlyMemory<byte> Body, 
    uint Attempt = 1,
    Func<ushort, byte[], CancellationToken, Task<byte[]>>? RequestFn = null) 
    : QueueItem(Route, Id, Token, Body, Attempt)
{
    internal QueueReservedItem(
        QueueItem item,
        Func<ushort, byte[], CancellationToken, Task<byte[]>> requestFn)
        : this(item.Route, item.Id, item.Token, item.Body, item.Attempt, requestFn)
    {
    }

    private Func<ushort, byte[], CancellationToken, Task<byte[]>> GetRequest()
    {
        return RequestFn ?? throw new InvalidOperationException("Request function not configured");
    }

    public override async Task ExtendAsync(ulong leaseSeconds, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(Id);
        writer.WriteU64(Token);
        writer.WriteU64(leaseSeconds);

        var response = await GetRequest()(MessageTypes.QueueExtend, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"EXTEND failed with status {status}", "EXTEND_FAILED", status);
        }
    }

    public override async Task CompleteAsync(CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(Id);
        writer.WriteU64(Token);

        var response = await GetRequest()(MessageTypes.QueueComplete, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"COMPLETE failed with status {status}", "COMPLETE_FAILED", status);
        }
    }

    public override async Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(Id);
        writer.WriteU64(token);

        var response = await GetRequest()(MessageTypes.QueueComplete, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"COMPLETE failed with status {status}", "COMPLETE_FAILED", status);
        }
    }
}
