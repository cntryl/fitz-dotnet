using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

public sealed class KvTransaction : IKvTransaction
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    private readonly string _route;
    private readonly ulong _txId;
    private readonly IDisposable? _disconnectRegistration;
    private int _closed;

    internal KvTransaction(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        string route,
        ulong txId,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retryRequest = null)
    {
        _request = request;
        _route = route;
        _txId = txId;
        _retryRequest = retryRequest;
        _disconnectRegistration = registerOnDisconnect?.Invoke(MarkClosed);
    }

    public async Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        ThrowIfClosed();
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key.Span);

        var response = await RequestWithRetryAsync(
            new RetryOperation("kv", "get", RetryClass.ReplayableRead),
            MessageTypes.KvGet,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
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
        ThrowIfClosed();
        return WriteAsync(MessageTypes.KvPut, key, value, "PUT", ct);
    }

    public Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default)
    {
        ThrowIfClosed();
        return WriteAsync(MessageTypes.KvInsert, key, value, "INSERT", ct);
    }

    public Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        ThrowIfClosed();
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key.Span);
        return ExpectStatusAsync(MessageTypes.KvDelete, writer, "DELETE", ct);
    }

    public Task DeleteRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, CancellationToken ct = default)
    {
        ThrowIfClosed();
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)startKey.Length);
        writer.WriteBytes(startKey.Span);
        writer.WriteU32((uint)endKey.Length);
        writer.WriteBytes(endKey.Span);
        return ExpectStatusAsync(MessageTypes.KvDeleteRange, writer, "DELETE_RANGE", ct);
    }

    public async IAsyncEnumerable<KvPair> ScanAsync(KvScanQuery query, [EnumeratorCancellation] CancellationToken ct = default)
    {
        ThrowIfClosed();
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

        var response = await RequestWithRetryAsync(
            new RetryOperation("kv", "scan", RetryClass.ReplayableRead),
            MessageTypes.KvScan,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
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
        ThrowIfClosed();
        return FinalizeAsync(MessageTypes.KvCommit, "COMMIT", ct);
    }

    public Task RollbackAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            return Task.CompletedTask;
        }

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
        MarkClosed();
    }

    private async Task ExpectStatusAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        ThrowIfClosed();
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

    private Task ExpectStatusAsync(ushort messageType, BinaryBufferWriter writer, string operation, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return ExpectStatusWithOwnedWriterAsync(messageType, writer, operation, ct);
    }

    private async Task ExpectStatusWithOwnedWriterAsync(ushort messageType, BinaryBufferWriter writer, string operation, CancellationToken ct)
    {
        using (writer)
        {
            await ExpectStatusAsync(messageType, writer.WrittenMemory, operation, ct).ConfigureAwait(false);
        }
    }

    private ValueTask<ReadOnlyMemory<byte>> RequestWithRetryAsync(
        RetryOperation operation,
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        if (_retryRequest is null)
        {
            return _request(messageType, payload, cancellationToken);
        }

        return _retryRequest(operation, messageType, payload, cancellationToken);
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) == 0)
        {
            return;
        }

        throw new KvException("Transaction is no longer valid after disconnect", "TX_CLOSED");
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
