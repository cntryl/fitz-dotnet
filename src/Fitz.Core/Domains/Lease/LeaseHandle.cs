using System.Threading;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseHandle : ILease
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    IDisposable? _disconnectRegistration;
    int _closed;
    int _disposed;
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposal may race active operations; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _operationGate = new(1, 1);
    readonly TaskCompletionSource _connectionLost = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource _fencingTokenChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal LeaseHandle(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        string route,
        ulong token,
        Func<Action, IDisposable>? registerOnDisconnect = null)
    {
        _request = request;
        Route = route;
        FencingToken = token;
        var disconnectRegistration = registerOnDisconnect?.Invoke(MarkConnectionLost);
        if (disconnectRegistration is not null)
        {
            Interlocked.Exchange(ref _disconnectRegistration, disconnectRegistration)?.Dispose();
            if (Volatile.Read(ref _closed) != 0)
            {
                Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
            }
        }
    }

    public string Route { get; }

    public ulong FencingToken { get; private set; }

    internal Task ConnectionLost => _connectionLost.Task;

    internal Task FencingTokenChanged => _fencingTokenChanged.Task;

    internal void Invalidate() => MarkClosed();

    public async Task ExtendAsync(ulong ttlSecs, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfZero(ttlSecs, nameof(ttlSecs));
        await _operationGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            await SendTokenTtlAsync(MessageTypes.LeaseRenew, ttlSecs, "EXTEND", ct).ConfigureAwait(false);
        }
        finally
        {
            _operationGate.Release();
        }
    }

    public async Task ReleaseAsync(CancellationToken ct = default)
    {
        await _operationGate.WaitAsync(ct).ConfigureAwait(false);
        try
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
        finally
        {
            _operationGate.Release();
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Lease disposal is bounded best-effort cleanup and must not replace an exception leaving an await-using scope.")]
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
                    await ReleaseAsync(cleanupDeadline.Token).ConfigureAwait(false);
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
        if (Volatile.Read(ref _closed) == 0)
        {
            return;
        }

        throw new LeaseException("Lease handle is no longer valid after disconnect", "CLOSED");
    }

    void MarkClosed() => _ = TryMarkClosed();

    void MarkConnectionLost()
    {
        if (!TryMarkClosed())
        {
            return;
        }

        _connectionLost.TrySetResult();
    }

    bool TryMarkClosed()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
        {
            return false;
        }

        Interlocked.Exchange(ref _disconnectRegistration, null)?.Dispose();
        return true;
    }
}
