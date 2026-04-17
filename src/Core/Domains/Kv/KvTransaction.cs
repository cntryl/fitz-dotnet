using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

public sealed class KvTransaction : IKvTransaction
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly string _route;
    private readonly ulong _txId;

    internal KvTransaction(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        string route,
        ulong txId)
    {
        _request = request;
        _route = route;
        _txId = txId;
    }

    public async Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key.Span);

        var response = await _request(MessageTypes.KvGet, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new KvException($"GET failed with status {status}", "GET_FAILED", status);
        }

        if (reader.IsEof)
        {
            return new KvGetResult(false, null);
        }

        var found = reader.ReadU8();
        if (found != 1)
        {
            if (!reader.IsEof)
            {
                throw new KvException("GET response has trailing bytes", "GET_INVALID_RESPONSE");
            }

            return new KvGetResult(false, null);
        }

        if (reader.RemainingBytes < 4)
        {
            throw new KvException("GET response missing value length", "GET_INVALID_RESPONSE");
        }

        var valueLength = reader.ReadU32();
        if (reader.RemainingBytes < valueLength)
        {
            throw new KvException("GET response truncated value", "GET_INVALID_RESPONSE");
        }

        var value = reader.ReadBytes((int)valueLength);
        if (!reader.IsEof)
        {
            throw new KvException("GET response has trailing bytes", "GET_INVALID_RESPONSE");
        }

        return new KvGetResult(true, value);
    }

    public Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default)
    {
        return WriteAsync(MessageTypes.KvPut, key, value, "PUT", ct);
    }

    public Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default)
    {
        return WriteAsync(MessageTypes.KvInsert, key, value, "INSERT", ct);
    }

    public Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key.Span);
        return ExpectStatusAsync(MessageTypes.KvDelete, writer.Build(), "DELETE", ct);
    }

    public Task DeleteRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)startKey.Length);
        writer.WriteBytes(startKey.Span);
        writer.WriteU32((uint)endKey.Length);
        writer.WriteBytes(endKey.Span);
        return ExpectStatusAsync(MessageTypes.KvDeleteRange, writer.Build(), "DELETE_RANGE", ct);
    }

    public async IAsyncEnumerable<KvPair> ScanAsync(KvScanQuery query, [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);

        // Encode optional start key
        if (query.StartKey.HasValue)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)query.StartKey.Value.Length);
            writer.WriteBytes(query.StartKey.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }

        // Encode optional end key
        if (query.EndKey.HasValue)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)query.EndKey.Value.Length);
            writer.WriteBytes(query.EndKey.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }

        // Encode optional limit
        writer.WriteU8(query.Limit.HasValue ? (byte)1 : (byte)0);
        if (query.Limit.HasValue)
        {
            writer.WriteU64(query.Limit.Value);
        }

        // Encode reverse flag
        writer.WriteU8(query.Reverse ? (byte)1 : (byte)0);

        var response = await _request(MessageTypes.KvScan, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new KvException($"SCAN failed with status {status}", "SCAN_FAILED", status);
        }

        // Read pairs count and parse all pairs
        var pairCount = reader.ReadU32();
        for (var i = 0; i < pairCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            var keyPath = reader.ReadBytes((int)reader.ReadU32());
            var value = reader.ReadBytes((int)reader.ReadU32());
            yield return new KvPair(keyPath, value);
        }

        if (!reader.IsEof)
        {
            throw new KvException("SCAN response has trailing bytes", "SCAN_INVALID_RESPONSE");
        }
    }

    public Task CommitAsync(CancellationToken ct = default)
    {
        return FinalizeAsync(MessageTypes.KvCommit, "COMMIT", ct);
    }

    public Task RollbackAsync(CancellationToken ct = default)
    {
        return FinalizeAsync(MessageTypes.KvRollback, "ROLLBACK", ct);
    }

    private async Task WriteAsync(ushort messageType, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, string operation, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key.Span);
        writer.WriteU32((uint)value.Length);
        writer.WriteBytes(value.Span);
        await ExpectStatusAsync(messageType, writer.WrittenMemory, operation, ct).ConfigureAwait(false);
    }

    private async Task FinalizeAsync(ushort messageType, string operation, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        await ExpectStatusAsync(messageType, writer.WrittenMemory, operation, ct).ConfigureAwait(false);
    }

    private async Task ExpectStatusAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new KvException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        if (!reader.IsEof)
        {
            throw new KvException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }
    }
}