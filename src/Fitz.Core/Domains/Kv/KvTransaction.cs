using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

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
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    readonly Func<bool> _supportsExclusiveScan;
    readonly TimeProvider _time;
    readonly string _route;
    readonly ulong _txId;
    readonly SessionLatch _session;
    readonly SemaphoreSlim _finalizationGate = new(1, 1);
    readonly SemaphoreSlim _operationGate = new(1, 1);
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    int _disposed;

    internal KvTransaction(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        string route,
        ulong txId,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retryRequest = null,
        Func<bool>? supportsExclusiveScan = null,
        TimeProvider? timeProvider = null)
    {
        _request = request;
        _time = timeProvider ?? TimeProvider.System;
        _closingToken = _closing.Token;
        _route = route;
        _txId = txId;
        _retryRequest = retryRequest;
        _supportsExclusiveScan = supportsExclusiveScan ?? (() => true);
        _session = new SessionLatch(registerOnDisconnect);
    }

    /// <inheritdoc />
    public string Route => _route;

    /// <inheritdoc />
    public bool SupportsExclusiveScan => _supportsExclusiveScan();

    /// <inheritdoc />
    public async Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        ThrowIfClosed();
        using var writer = Payload();
        writer.WriteLengthPrefixed(key.Span);

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
    public async Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default)
    {
        ThrowIfClosed();
        using var writer = Payload();
        writer.WriteLengthPrefixed(key.Span);
        await ExpectStatusAsync(MessageTypes.KvDelete, writer.WrittenMemory, "DELETE", ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task DeleteRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, CancellationToken ct = default)
    {
        ThrowIfClosed();
        using var writer = Payload();
        writer.WriteLengthPrefixed(startKey.Span);
        writer.WriteLengthPrefixed(endKey.Span);
        await ExpectStatusAsync(MessageTypes.KvDeleteRange, writer.WrittenMemory, "DELETE_RANGE", ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<KvScanResult> ScanAsync(KvScanQuery query, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        ThrowIfClosed();
        using var writer = Payload();
        writer.WriteOptionalLengthPrefixed(query.StartKey);
        writer.WriteOptionalLengthPrefixed(query.EndKey);
        writer.WriteU8(query.Limit.HasValue ? (byte)1 : (byte)0);
        if (query.Limit is { } limit)
        {
            writer.WriteU32(limit);
        }

        // Wire 3 combines reverse and exclusive-start into the final options byte.
        writer.WriteU8((byte)((query.Reverse ? 1 : 0) | (query.StartExclusive ? 2 : 0)));

        var response = await RequestWithRetryAsync(
            RetryOperations.KvScan,
            MessageTypes.KvScan,
            writer.WrittenMemory,
            ct).ConfigureAwait(false);
        var reader = KvWireHelpers.ReadSuccess(response, "SCAN");

        var pairCount = reader.ReadU32();
        if (reader.RemainingBytes < 1 || pairCount > (uint)((reader.RemainingBytes - 1) / 8))
        {
            throw new KvException("SCAN response pair count exceeds the remaining payload", "SCAN_INVALID_RESPONSE");
        }
        var pairs = new List<KvPair>(checked((int)pairCount));
        for (var i = 0; i < pairCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            var keyPath = reader.ReadLengthPrefixed();
            var value = reader.ReadLengthPrefixed();
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
    public Task CommitAsync(KvDurability durability, CancellationToken ct = default)
    {
        ThrowIfClosed();
        if (durability is not KvDurability.Buffered and not KvDurability.Sync)
        {
            throw new ArgumentOutOfRangeException(nameof(durability), durability, "Unknown KV commit persistence");
        }
        return FinalizeAsync(MessageTypes.KvCommit, "COMMIT", ct, durability);
    }

    /// <inheritdoc />
    public Task RollbackAsync(CancellationToken ct = default)
    {
        if (_session.IsClosed)
        {
            return Task.CompletedTask;
        }

        return FinalizeAsync(MessageTypes.KvRollback, "ROLLBACK", ct);
    }

    /// <summary>Rolls the transaction back if it has not been committed.</summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    /// <remarks>
    /// Waits for in-progress operations to finish first. Queued operations fail as closed,
    /// except a queued rollback, which returns because disposal rolls back.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _closing.CancelAsync().ConfigureAwait(false);
        await _finalizationGate.DrainAndDisposeAsync().ConfigureAwait(false);
        await _operationGate.DrainAndDisposeAsync().ConfigureAwait(false);
        _closing.Dispose();
        try
        {
            if (!_session.IsClosed)
            {
                using var cleanupDeadline = new CancellationTokenSource(CleanupBudget, _time);
                using var writer = FinalizationPayload(durability: null);
                await SendStatusAsync(MessageTypes.KvRollback, writer.WrittenMemory, "ROLLBACK", cleanupDeadline.Token).ConfigureAwait(false);
            }
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            // Best-effort cleanup must not replace an exception leaving an await-using scope.
        }
        finally
        {
            MarkClosed();
        }
    }

    async Task WriteAsync(ushort messageType, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, string operation, CancellationToken ct)
    {
        using var writer = Payload();
        writer.WriteLengthPrefixed(key.Span);
        writer.WriteLengthPrefixed(value.Span);
        await ExpectStatusAsync(messageType, writer.WrittenMemory, operation, ct).ConfigureAwait(false);
    }

    async Task FinalizeAsync(ushort messageType, string operation, CancellationToken ct, KvDurability? durability = null)
    {
        if (!await _finalizationGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
        {
            // Disposal is rolling the transaction back, which is what a rollback asked for.
            if (messageType == MessageTypes.KvRollback)
                return;

            throw ClosedException();
        }

        try
        {
            ThrowIfClosed();
            using var writer = FinalizationPayload(durability);
            await ExpectStatusAsync(messageType, writer.WrittenMemory, operation, ct, ensureOpen: false).ConfigureAwait(false);
            MarkClosed();
        }
        finally
        {
            _finalizationGate.Release();
        }
    }

    BinaryBufferWriter FinalizationPayload(KvDurability? durability)
    {
        var writer = Payload();
        if (durability is { } persistence)
        {
            writer.WriteU8((byte)persistence);
        }
        return writer;
    }

    async Task ExpectStatusAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct, bool ensureOpen = true)
    {
        await EnterOperationAsync(ct).ConfigureAwait(false);
        try
        {
            if (ensureOpen)
            {
                ThrowIfClosed();
            }
            await SendStatusAsync(messageType, payload, operation, ct).ConfigureAwait(false);
        }
        finally
        {
            _operationGate.Release();
        }
    }

    // Callers hold the operation gate, or have drained it during disposal.
    async Task SendStatusAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        var reader = KvWireHelpers.ReadSuccess(response, operation);
        if (!reader.IsEof)
        {
            throw new KvException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }
    }

    async Task EnterOperationAsync(CancellationToken ct)
    {
        if (!await _operationGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            throw ClosedException();
    }

    async ValueTask<ReadOnlyMemory<byte>> RequestWithRetryAsync(
        RetryOperation operation,
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        await EnterOperationAsync(ct).ConfigureAwait(false);
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

    // Every transaction request starts with the transaction id and route.
    BinaryBufferWriter Payload()
    {
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        return writer;
    }

    void ThrowIfClosed()
    {
        if (_session.IsClosed)
            throw ClosedException();
    }

    static KvException ClosedException() => new("Transaction is no longer valid after disconnect", "TX_CLOSED");

    void MarkClosed() => _session.TryClose();
}
