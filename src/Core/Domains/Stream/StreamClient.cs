using System.Runtime.CompilerServices;
using System.Text.Json;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamClient : IStreamClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;

    internal StreamClient(FitzConnection connection)
        : this(connection.RequestAsync, connection.RegisterNotificationHandler)
    {
    }

    public StreamClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
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

        var response = await _request(MessageTypes.StreamBegin, writer.Build(), ct).ConfigureAwait(false);
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

        var response = await _request(MessageTypes.StreamRead, writer.Build(), ct).ConfigureAwait(false);
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

    public async Task<StreamRecord?> PeekAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.StreamLast, writer.Build(), ct).ConfigureAwait(false);
        var (status, data) = ReadWrappedStreamResponse(response);
        if (status != 0)
        {
            throw new StreamException($"LAST failed with status {status}", "LAST_FAILED", status);
        }

        if (data.Length == 0)
        {
            return null;
        }

        var reader = new BinaryBufferReader(data);
        var offset = reader.ReadU64();
        var bodyLength = reader.ReadU32();
        var body = reader.ReadBytes((int)bodyLength);
        return new StreamRecord(offset, body);
    }

    public async Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.StreamGetMetadata, writer.Build(), ct).ConfigureAwait(false);
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

    public async IAsyncEnumerable<StreamCommitEvent> SubscribeAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamSubscribe, writer.Build(), ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new StreamException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
        }

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new StreamException("SUBSCRIBE response missing subscription id", "MISSING_SUBSCRIPTION_ID");
        }

        var subscriptionId = reader.ReadU64();
        var channel = new SubscriptionChannel<StreamCommitEvent>();
        var registration = _registerNotificationHandler(MessageTypes.StreamNotify, payload =>
        {
            try
            {
                var notifyReader = new BinaryBufferReader(payload);
                if (notifyReader.ReadU64() != subscriptionId)
                {
                    return;
                }

                var route = notifyReader.ReadString();
                var bodyLength = notifyReader.ReadU32();
                var body = notifyReader.ReadBytes((int)bodyLength);
                channel.PostNotification(new StreamCommitEvent(route, TryParseCommitOffset(body)));
            }
            catch
            {
                channel.Dispose();
            }
        });

        try
        {
            await foreach (var evt in channel.GetEnumerableAsync(ct).ConfigureAwait(false))
            {
                yield return evt;
            }
        }
        finally
        {
            registration.Dispose();
            channel.Dispose();
        }
    }

    private static ulong TryParseCommitOffset(byte[] payload)
    {
        try
        {
            using var document = JsonDocument.Parse(payload);
            if (document.RootElement.TryGetProperty("last_resource_offset", out var lastOffset))
            {
                return lastOffset.GetUInt64();
            }

            if (document.RootElement.TryGetProperty("first_resource_offset", out var firstOffset))
            {
                return firstOffset.GetUInt64();
            }
        }
        catch
        {
        }

        return 0;
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
