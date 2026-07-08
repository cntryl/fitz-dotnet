using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using System.Threading;

namespace Cntryl.Fitz.Domains.Queue;

internal sealed class QueueReservedItem : QueueItem
{
    private readonly ulong _id;
    private readonly ulong _token;
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _requestFn;
    private readonly IDisposable? _disconnectRegistration;
    private int _closed;

    internal QueueReservedItem(
        string route,
        ReadOnlyMemory<byte> body,
        uint attempt,
        ulong id,
        ulong token,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> requestFn,
        Func<Action, IDisposable>? registerOnDisconnect = null)
        : base(route, body, attempt)
    {
        _id = id;
        _token = token;
        _requestFn = requestFn;
        _disconnectRegistration = registerOnDisconnect?.Invoke(MarkClosed);
    }

    public override async Task ExtendAsync(ulong leaseSeconds, CancellationToken ct = default)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(_id);
        writer.WriteU64(_token);
        writer.WriteU64(leaseSeconds);

        var response = await _requestFn(MessageTypes.QueueExtend, writer.WrittenMemory, ct).ConfigureAwait(false);
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
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(_id);
        writer.WriteU64(_token);

        var response = await _requestFn(MessageTypes.QueueComplete, writer.WrittenMemory, ct).ConfigureAwait(false);
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
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(_id);
        writer.WriteU64(token);

        var response = await _requestFn(MessageTypes.QueueComplete, writer.WrittenMemory, ct).ConfigureAwait(false);
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

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) == 0)
        {
            return;
        }

        throw new QueueException("Queue item is no longer valid after disconnect", "ITEM_CLOSED");
    }

    private void MarkClosed()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
        {
            return;
        }

        _disconnectRegistration?.Dispose();
    }
}
