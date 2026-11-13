using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Lease;

/// <summary>
/// The default <see cref="ILease"/>. Obtained from <see cref="LeaseClient.AcquireAsync"/>.
/// </summary>
/// <remarks>
/// Fencing-token rotation is serialized, and the handle closes itself when a renewal outcome
/// is uncertain rather than assuming continued ownership.
/// </remarks>
sealed class LeaseHandle : ILease
{
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly SessionLatch _session;
    readonly TimeProvider _time;
    int _disposed;
    readonly SemaphoreSlim _operationGate = new(1, 1);
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    readonly TaskCompletionSource _connectionLost = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _fencingTokenChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal LeaseHandle(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        string route,
        ulong token,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        TimeProvider? timeProvider = null)
    {
        _request = request;
        _time = timeProvider ?? TimeProvider.System;
        _closingToken = _closing.Token;
        Route = route;
        FencingToken = token;
        _session = new SessionLatch(registerOnDisconnect, () => _connectionLost.TrySetResult());
    }

    /// <inheritdoc />
    public string Route { get; }

    /// <inheritdoc />
    public ulong FencingToken { get; private set; }

    internal Task ConnectionLost => _connectionLost.Task;

    internal Task FencingTokenChanged => _fencingTokenChanged.Task;

    internal void Invalidate() => MarkClosed();

    /// <inheritdoc />
    public async Task ExtendAsync(TimeSpan ttl, CancellationToken ct = default)
    {
        var ttlSecs = WireDuration.ToSeconds(ttl, nameof(ttl));
        ArgumentOutOfRangeException.ThrowIfZero(ttlSecs, nameof(ttl));
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            try
            {
                await SendTokenTtlAsync(MessageTypes.LeaseRenew, ttlSecs, "EXTEND", ct).ConfigureAwait(false);
            }
            catch (Exception error) when (error is not LeaseException { Status: 1, ErrorEffect: 0 })
            {
                MarkClosed();
                throw;
            }
        }
        finally
        {
            _operationGate.Release();
        }
    }

    /// <inheritdoc />
    public async Task ReleaseAsync(CancellationToken ct = default)
    {
        await EnterAsync(ct).ConfigureAwait(false);
        try
        {
            await ReleaseCoreAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _operationGate.Release();
        }
    }

    /// <summary>Releases the lease on a best-effort, bounded basis.</summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    /// <remarks>
    /// Waits for an in-progress extend or release to finish first; queued calls fail as closed.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _closing.CancelAsync().ConfigureAwait(false);
        await _operationGate.DrainAndDisposeAsync().ConfigureAwait(false);
        _closing.Dispose();
        try
        {
            if (!_session.IsClosed)
            {
                using var cleanupDeadline = new CancellationTokenSource(CleanupBudget, _time);
                await ReleaseCoreAsync(cleanupDeadline.Token).ConfigureAwait(false);
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

    async Task EnterAsync(CancellationToken ct)
    {
        if (!await _operationGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            throw ClosedException();
    }

    // Callers hold the gate, or have drained it during disposal.
    async Task ReleaseCoreAsync(CancellationToken ct)
    {
        ThrowIfClosed();
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteString(string.Empty);
        writer.WriteU64(FencingToken);
        var response = await _request(MessageTypes.LeaseRelease, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = LeaseWireHelpers.ReadSuccess(response, "RELEASE");
        if (!reader.IsEof)
        {
            throw new LeaseException("RELEASE response has trailing bytes", "RELEASE_INVALID_RESPONSE");
        }

        MarkClosed();
    }

    async Task SendTokenTtlAsync(ushort messageType, ulong ttlSecs, string operation, CancellationToken ct)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteString(string.Empty);
        writer.WriteU64(FencingToken);
        writer.WriteU64(ttlSecs);
        var response = await _request(messageType, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = LeaseWireHelpers.ReadSuccess(response, operation);
        if (reader.RemainingBytes >= 8)
        {
            var previousToken = FencingToken;
            var renewedToken = reader.ReadU64();
            if (!reader.IsEof)
            {
                throw new LeaseException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
            }

            FencingToken = renewedToken;
            if (renewedToken != previousToken)
            {
                _fencingTokenChanged.TrySetResult();
            }
        }
        else
        {
            throw new LeaseException($"{operation} response missing fencing token", $"{operation}_INVALID_RESPONSE");
        }
    }

    void ThrowIfClosed()
    {
        if (_session.IsClosed)
            throw ClosedException();
    }

    static LeaseException ClosedException() =>
        new("Lease handle is no longer valid after disconnect", "CLOSED");

    void MarkClosed() => _session.TryClose();
}
