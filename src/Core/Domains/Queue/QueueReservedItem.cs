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
    Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? RequestFn = null) 
    : QueueItem(Route, Id, Token, Body, Attempt)
{
    internal QueueReservedItem(
        QueueItem item,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> requestFn)
        : this(item.Route, item.Id, item.Token, item.Body, item.Attempt, requestFn)
    {
    }

    private Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> GetRequest()
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

        var response = await GetRequest()(MessageTypes.QueueExtend, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"EXTEND failed with status {status}", "EXTEND_FAILED", status);
        }

        if (!reader.IsEof)
        {
            throw new QueueException("EXTEND response has trailing bytes", "EXTEND_INVALID_RESPONSE");
        }
    }

    public override async Task CompleteAsync(CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(Id);
        writer.WriteU64(Token);

        var response = await GetRequest()(MessageTypes.QueueComplete, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"COMPLETE failed with status {status}", "COMPLETE_FAILED", status);
        }

        if (!reader.IsEof)
        {
            throw new QueueException("COMPLETE response has trailing bytes", "COMPLETE_INVALID_RESPONSE");
        }
    }

    public override async Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(Id);
        writer.WriteU64(token);

        var response = await GetRequest()(MessageTypes.QueueComplete, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"COMPLETE failed with status {status}", "COMPLETE_FAILED", status);
        }

        if (!reader.IsEof)
        {
            throw new QueueException("COMPLETE response has trailing bytes", "COMPLETE_INVALID_RESPONSE");
        }
    }
}
