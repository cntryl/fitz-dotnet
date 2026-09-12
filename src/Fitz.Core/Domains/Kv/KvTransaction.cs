using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

/// <summary>
/// The default <see cref="IKvTransaction"/>. Obtained from <see cref="KvClient.BeginAsync"/>.
/// </summary>
/// <remarks>
/// Operations on one transaction are serialized. Commit becomes terminal only after broker
/// success; a definite rejection leaves the transaction retryable.
/// </remarks>
sealed class KvTransaction : IKvTransaction
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    readonly string _route;
    readonly ulong _txId;
    IDisposable? _disconnectRegistration;
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposal may race active operations; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _finalizationGate = new(1, 1);
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposal may race active operations; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _operationGate = new(1, 1);
    int _closed;
    int _disposed;

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
        var disconnectRegistration = registerOnDisconnect?.Invoke(MarkClosed);
        if (disconnectRegistration is not null)
        {
            Interlocked.Exchange(ref _disconnectRegistration, disconnectRegistration)?.Dispose();
            if (Volatile.Read(ref _closed) != 0)
            {
                Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
            }
        }
    }

    /// <inheritdoc />
    public async Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        ThrowIfClosed();
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key.Span);

        var response = await RequestWithRetryAsync(
            RetryOperations.KvGet,
            MessageTypes.KvGet,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var reader = KvWireHelpers.ReadSuccess(response, "GET");

        if (reader.IsEof)
        {
            return new KvGetResult(false, null);
        }

        var found = reader.ReadU8();
        if (found > 1)
        {
            throw new KvException($"GET response has invalid found flag {found}", "GET_INVALID_RESPONSE");
        }

        if (found != 1)
        {
            if (reader.IsEof)
            {
                return new KvGetResult(false, null);
            }

            if (reader.RemainingBytes != 4 || reader.ReadU32() != 0 || !reader.IsEof)
            {
                throw new KvException("GET not-found response has invalid value length", "GET_INVALID_RESPONSE");
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

        var value = reader.ReadBytes(valueLength);
        if (!reader.IsEof)
        {
            throw new KvException("GET response has trailing bytes", "GET_INVALID_RESPONSE");
        }

        return new KvGetResult(true, value);
    }

    /// <inheritdoc />
    public Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default)
    {
        ThrowIfClosed();
        return WriteAsync(MessageTypes.KvPut, key, value, "PUT", ct);
    }

    /// <inheritdoc />
    public Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default)
    {
        ThrowIfClosed();
        return WriteAsync(MessageTypes.KvInsert, key, value, "INSERT", ct);
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task<KvScanResult> ScanAsync(KvScanQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
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
            RetryOperations.KvScan,
            MessageTypes.KvScan,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var reader = KvWireHelpers.ReadSuccess(response, "SCAN");

        var pairCount = reader.ReadU32();
        if (pairCount > (uint)(reader.RemainingBytes / 8))
        {
            throw new KvException("SCAN response pair count exceeds the remaining payload", "SCAN_INVALID_RESPONSE");
        }
        var pairs = new List<KvPair>(checked((int)pairCount));
        for (var i = 0; i < pairCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            var keyPath = reader.ReadBytes(reader.ReadU32());
            var value = reader.ReadBytes(reader.ReadU32());
            pairs.Add(new KvPair(keyPath, value));
        }

        if (reader.RemainingBytes != 1)
        {
            throw new KvException("SCAN response missing has_more", "SCAN_INVALID_RESPONSE");
        }
        var hasMoreByte = reader.ReadU8();
        if (hasMoreByte > 1)
        {
            throw new KvException("SCAN response has invalid has_more", "SCAN_INVALID_RESPONSE");
        }
        if (!reader.IsEof)
        {
            throw new KvException("SCAN response has trailing bytes", "SCAN_INVALID_RESPONSE");
        }
        return new KvScanResult(pairs, hasMoreByte == 1);
    }

    /// <inheritdoc />
    public Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfClosed();
        return FinalizeAsync(MessageTypes.KvCommit, "COMMIT", ct);
    }

    /// <inheritdoc />
    public Task RollbackAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            return Task.CompletedTask;
        }

        return FinalizeAsync(MessageTypes.KvRollback, "ROLLBACK", ct);
    }

    /// <summary>Rolls the transaction back if it has not been committed.</summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Transaction disposal is bounded best-effort cleanup and must not replace an exception leaving an await-using scope.")]
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            if (Volatile.Read(ref _closed) == 0)
            {
                try
                {
                    using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    await RollbackAsync(cleanupDeadline.Token).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort cleanup must not replace an exception leaving an await-using scope.
                }
            }
        }
        finally
        {
            MarkClosed();
            GC.SuppressFinalize(this);
        }
    }

    async Task WriteAsync(ushort messageType, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, string operation, CancellationToken ct)
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

    async Task FinalizeAsync(ushort messageType, string operation, CancellationToken ct)
    {
        await _finalizationGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            using var writer = new BinaryBufferWriter();
            writer.WriteU64(_txId);
            writer.WriteString(_route);
            await ExpectStatusAsync(messageType, writer.WrittenMemory, operation, ct, ensureOpen: false).ConfigureAwait(false);
            MarkClosed();
        }
        finally
        {
            _finalizationGate.Release();
        }
    }

    async Task ExpectStatusAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct, bool ensureOpen = true)
    {
        await _operationGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (ensureOpen)
            {
                ThrowIfClosed();
            }
            var response = await _request(messageType, payload, ct).ConfigureAwait(false);
            var reader = KvWireHelpers.ReadSuccess(response, operation);

            if (!reader.IsEof)
            {
                throw new KvException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
            }
        }
        finally
        {
            _operationGate.Release();
        }
    }

    Task ExpectStatusAsync(ushort messageType, BinaryBufferWriter writer, string operation, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(writer);
        return ExpectStatusWithOwnedWriterAsync(messageType, writer, operation, ct);
    }

    async Task ExpectStatusWithOwnedWriterAsync(ushort messageType, BinaryBufferWriter writer, string operation, CancellationToken ct)
    {
        using (writer)
        {
            await ExpectStatusAsync(messageType, writer.WrittenMemory, operation, ct).ConfigureAwait(false);
        }
    }

    async ValueTask<ReadOnlyMemory<byte>> RequestWithRetryAsync(
        RetryOperation operation,
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        await _operationGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            if (_retryRequest is null)
            {
                return await _request(messageType, payload, ct).ConfigureAwait(false);
            }

            return await _retryRequest(operation, messageType, payload, ct).ConfigureAwait(false);
        }
        finally
        {
            _operationGate.Release();
        }
    }

    void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) == 0)
        {
            return;
        }

        throw new KvException("Transaction is no longer valid after disconnect", "TX_CLOSED");
    }

    void MarkClosed()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
        {
            return;
        }

        Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
    }
}
