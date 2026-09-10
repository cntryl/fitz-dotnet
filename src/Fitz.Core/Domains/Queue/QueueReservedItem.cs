using System.Threading;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Queue;

sealed class QueueReservedItem : QueueItem
{
    readonly ulong _id;
    readonly ulong _token;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _requestFn;
    IDisposable? _disconnectRegistration;
    int _state;

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
        var disconnectRegistration = registerOnDisconnect?.Invoke(MarkClosed);
        if (disconnectRegistration is not null)
        {
            Interlocked.Exchange(ref _disconnectRegistration, disconnectRegistration)?.Dispose();
            if (Volatile.Read(ref _state) != 0)
            {
                Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
            }
        }
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
            var message = reader.ReadString();
            if (!reader.IsEof)
            {
                throw new QueueException("EXTEND error response has trailing bytes", "EXTEND_INVALID_RESPONSE");
            }
            throw new QueueException($"EXTEND failed: {message}", "EXTEND_FAILED", status);
        }

        if (!reader.IsEof)
        {
            throw new QueueException("EXTEND response has trailing bytes", "EXTEND_INVALID_RESPONSE");
        }
    }

    public override Task CompleteAsync(CancellationToken ct = default) => CompleteCoreAsync(_token, ct);

    public override Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default) => CompleteCoreAsync(token, ct);

    public override ValueTask DisposeAsync()
    {
        MarkClosed();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    async Task CompleteCoreAsync(ulong token, CancellationToken ct)
    {
        BeginCompletion();
        try
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteString(Route);
            writer.WriteU64(_id);
            writer.WriteU64(token);

            var response = await _requestFn(MessageTypes.QueueComplete, writer.WrittenMemory, ct).ConfigureAwait(false);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                var message = reader.ReadString();
                if (!reader.IsEof)
                {
                    throw new QueueException("COMPLETE error response has trailing bytes", "COMPLETE_INVALID_RESPONSE");
                }
                throw new QueueException($"COMPLETE failed: {message}", "COMPLETE_FAILED", status);
            }

            if (!reader.IsEof)
            {
                throw new QueueException("COMPLETE response has trailing bytes", "COMPLETE_INVALID_RESPONSE");
            }

            MarkClosed();
        }
        finally
        {
            Interlocked.CompareExchange(ref _state, 0, 1);
        }
    }

    void ThrowIfClosed()
    {
        if (Volatile.Read(ref _state) == 0)
        {
            return;
        }

        throw new QueueException("Queue item is no longer valid after disconnect", "ITEM_CLOSED");
    }

    void MarkClosed()
    {
        if (Interlocked.Exchange(ref _state, 2) == 2)
        {
            return;
        }

        Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
    }

    void BeginCompletion()
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) == 0)
        {
            return;
        }

        throw new QueueException("Queue item is completing or closed", "ITEM_CLOSED");
    }
}
