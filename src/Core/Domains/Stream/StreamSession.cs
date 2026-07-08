using System.Threading;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamSession : IStreamSession
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly ulong _sessionId;
    private readonly IDisposable? _disconnectRegistration;
    private int _closed;

    internal StreamSession(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        ulong sessionId,
        Func<Action, IDisposable>? registerOnDisconnect = null)
    {
        _request = request;
        _sessionId = sessionId;
        _disconnectRegistration = registerOnDisconnect?.Invoke(MarkClosed);
    }

    public async Task<ulong?> AppendAsync(ulong expectedOffset, ReadOnlyMemory<byte> body, ReadOnlyMemory<byte>? metadata = null, string? discriminator = null, CancellationToken ct = default)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        writer.WriteU64(expectedOffset);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);
        if (metadata.HasValue && metadata.Value.Length > 0)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)metadata.Value.Length);
            writer.WriteBytes(metadata.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }

        if (!string.IsNullOrEmpty(discriminator))
        {
            writer.WriteU8(1);
            writer.WriteString(discriminator);
        }
        else
        {
            writer.WriteU8(0);
        }

        var response = await _request(MessageTypes.StreamAppend, writer.WrittenMemory, ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "APPEND");
        if (data.IsEmpty || data.Length < 8)
        {
            return null;
        }

        var wrapped = new BinaryBufferReader(data);
        return wrapped.ReadU64();
    }

    public Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        writer.WriteU8(0);
        return ExpectStatusAsync(MessageTypes.StreamCommit, writer.WrittenMemory, "COMMIT", ct);
    }

    public Task RollbackAsync(CancellationToken ct = default)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        return ExpectStatusAsync(MessageTypes.StreamRollback, writer.WrittenMemory, "ROLLBACK", ct);
    }

    private async Task ExpectStatusAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        ThrowIfClosed();
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        StreamWireHelpers.EnsureSuccessStatusOnly(response, operation);
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) == 0)
        {
            return;
        }

        throw new StreamException("Stream session already closed", "SESSION_CLOSED");
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
