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

    public async Task<IStreamSession> BeginAsync(string route, ulong expectedOffset = 0, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(expectedOffset);
        if (ingestMetadata.HasValue && ingestMetadata.Value.Length > 0)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)ingestMetadata.Value.Length);
            writer.WriteBytes(ingestMetadata.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }

        var response = await _request(MessageTypes.StreamBegin, writer.Build(), ct);
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
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(startOffset);
        writer.WriteU64(limit);
        writer.WriteU8((byte)(maxBytes.HasValue ? 1 : 0));
        if (maxBytes.HasValue)
        {
            writer.WriteU64(maxBytes.Value);
        }

        var response = await _request(MessageTypes.StreamRead, writer.Build(), ct);
        var (status, data) = ReadWrappedStreamResponse(response);
        if (status != 0)
        {
            throw new StreamException($"READ failed with status {status}", "READ_FAILED", status);
        }

        if (data.Length == 0)
        {
            yield break;
        }

        var records = ParseReadRecords(data);
        foreach (var record in records)
        {
            yield return record;
        }
    }

    public async Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.StreamGetMetadata, writer.Build(), ct);
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

    private static List<StreamRecord> ParseReadRecords(byte[] data)
    {
        // Prefer count-prefixed parsing. Fall back to flat parsing for brokers
        // that return records directly without a leading count.
        var fromCount = TryParseCountPrefixedRecords(data);
        if (fromCount.Count > 0)
        {
            return fromCount;
        }

        return ParseFlatRecords(data);
    }

    private static List<StreamRecord> TryParseCountPrefixedRecords(byte[] data)
    {
        var records = new List<StreamRecord>();
        if (data.Length < 4)
        {
            return records;
        }

        var reader = new BinaryBufferReader(data);
        uint count;
        try
        {
            count = reader.ReadU32();
        }
        catch
        {
            return records;
        }

        for (var index = 0U; index < count; index++)
        {
            try
            {
                var offset = reader.ReadU64();
                var bodyLength = reader.ReadU32();
                var body = reader.ReadBytes((int)bodyLength);
                records.Add(new StreamRecord(offset, body));
            }
            catch
            {
                records.Clear();
                return records;
            }
        }

        return reader.IsEof ? records : new List<StreamRecord>();
    }

    private static List<StreamRecord> ParseFlatRecords(byte[] data)
    {
        var records = new List<StreamRecord>();
        if (data.Length < 12)
        {
            return records;
        }

        var reader = new BinaryBufferReader(data);
        while (!reader.IsEof)
        {
            try
            {
                var offset = reader.ReadU64();
                var bodyLength = reader.ReadU32();
                var body = reader.ReadBytes((int)bodyLength);
                records.Add(new StreamRecord(offset, body));
            }
            catch
            {
                break;
            }
        }

        return records;
    }
}