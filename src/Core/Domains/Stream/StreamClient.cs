using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using System.Runtime.CompilerServices;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamClient : IStreamClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

    internal StreamClient(FitzConnection connection)
        : this(connection.RequestAsync)
    {
    }

    public StreamClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
    {
        _request = request;
    }

    public async Task<IStreamSession> BeginAsync(string route, ulong expectedOffset = 0, byte[]? ingestMetadata = null, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(expectedOffset);
        if (ingestMetadata is { Length: > 0 })
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)ingestMetadata.Length);
            writer.WriteBytes(ingestMetadata);
        }
        else
        {
            writer.WriteU8(0);
        }

        var response = await _request(MessageTypes.StreamBegin, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new StreamException($"BEGIN failed with status {status}", "BEGIN_FAILED", status);
        }

        var hasSession = reader.IsEof ? (byte)0 : reader.ReadU8();
        if (hasSession != 1 || reader.RemainingBytes < 8)
        {
            throw new StreamException("BEGIN response missing session id", "MISSING_SESSION_ID");
        }

        return new StreamSession(_request, reader.ReadU64());
    }

    public async IAsyncEnumerable<StreamRecord> ReadAsync(
        string route,
        ulong startOffset,
        ulong limit = 100,
        ulong? maxBytes = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(startOffset);
        writer.WriteU64(limit);
        writer.WriteU8((byte)(maxBytes.HasValue ? 1 : 0));
        if (maxBytes.HasValue)
        {
            writer.WriteU64(maxBytes.Value);
        }

        var response = await _request(MessageTypes.StreamRead, writer.Build(), cancellationToken);
        var (status, data) = ReadWrappedStreamResponse(response);
        if (status != 0)
        {
            throw new StreamException($"READ failed with status {status}", "READ_FAILED", status);
        }

        if (data.Length == 0)
        {
            yield break;
        }

        var inner = new BinaryBufferReader(data);
        var count = inner.ReadU32();
        for (var index = 0; index < count; index++)
        {
            yield return new StreamRecord(inner.ReadU64(), inner.ReadBytes((int)inner.ReadU32()));
        }
    }

    public async Task<StreamMetadata> MetadataAsync(string route, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.StreamGetMetadata, writer.Build(), cancellationToken);
        var (status, data) = ReadWrappedStreamResponse(response);
        if (status != 0)
        {
            throw new StreamException($"METADATA failed with status {status}", "METADATA_FAILED", status);
        }

        if (data.Length == 0)
        {
            return new StreamMetadata(0, 0, 0);
        }

        var inner = new BinaryBufferReader(data);
        return new StreamMetadata(inner.ReadU64(), inner.ReadU64(), inner.ReadU64());
    }

    private static (byte Status, byte[] Data) ReadWrappedStreamResponse(byte[] response)
    {
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            return (status, []);
        }

        if (!reader.IsEof)
        {
            var hasSession = reader.ReadU8();
            if (hasSession == 1 && reader.RemainingBytes >= 8)
            {
                _ = reader.ReadU64();
            }
        }

        return reader.IsEof ? ((byte)0, []) : ((byte)0, reader.ReadBytes((int)reader.ReadU32()));
    }
}