using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamSession : IStreamSession
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly ulong _sessionId;

    internal StreamSession(Func<ushort, byte[], CancellationToken, Task<byte[]>> request, ulong sessionId)
    {
        _request = request;
        _sessionId = sessionId;
    }

    public async Task<ulong?> AppendAsync(byte[] body, byte[]? metadata = null, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body);
        if (metadata is { Length: > 0 })
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)metadata.Length);
            writer.WriteBytes(metadata);
        }
        else
        {
            writer.WriteU8(0);
        }

        var response = await _request(MessageTypes.StreamAppend, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new StreamException($"APPEND failed with status {status}", "APPEND_FAILED", status);
        }

        if (!reader.IsEof)
        {
            var hasSession = reader.ReadU8();
            if (hasSession == 1 && reader.RemainingBytes >= 8)
            {
                _ = reader.ReadU64();
            }
        }

        if (reader.IsEof)
        {
            return null;
        }

        var wrapped = new BinaryBufferReader(reader.ReadBytes((int)reader.ReadU32()));
        return wrapped.RemainingBytes >= 8 ? wrapped.ReadU64() : null;
    }

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        writer.WriteU8(0);
        return ExpectStatusAsync(MessageTypes.StreamCommit, writer.Build(), "COMMIT", cancellationToken);
    }

    public Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        return ExpectStatusAsync(MessageTypes.StreamRollback, writer.Build(), "ROLLBACK", cancellationToken);
    }

    private async Task ExpectStatusAsync(ushort messageType, byte[] payload, string operation, CancellationToken cancellationToken)
    {
        var response = await _request(messageType, payload, cancellationToken);
        if (response.Length > 0 && response[0] != 0)
        {
            throw new StreamException($"{operation} failed with status {response[0]}", $"{operation}_FAILED", response[0]);
        }
    }
}