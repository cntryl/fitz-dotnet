using System.Threading;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseHandle : ILease
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly IDisposable? _disconnectRegistration;
    private int _closed;
    private int _disposed;
    private readonly SemaphoreSlim _operationGate = new(1, 1);

    internal LeaseHandle(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        string route,
        ulong token,
        Func<Action, IDisposable>? registerOnDisconnect = null)
    {
        _request = request;
        Route = route;
        Token = token;
        _disconnectRegistration = registerOnDisconnect?.Invoke(MarkClosed);
    }

    public string Route { get; }

    internal ulong Token { get; private set; }

    public async Task ExtendAsync(ulong ttlSecs, CancellationToken ct = default)
    {
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
            MarkClosed();

            using var writer = new BinaryBufferWriter();
            writer.WriteString(Route);
            writer.WriteString(string.Empty);
            writer.WriteU64(Token);
            var response = await _request(MessageTypes.LeaseRelease, writer.WrittenMemory, ct).ConfigureAwait(false);
            var reader = LeaseWireHelpers.ReadSuccess(response, "RELEASE");
            if (!reader.IsEof)
            {
                throw new LeaseException("RELEASE response has trailing bytes", "RELEASE_INVALID_RESPONSE");
            }
        }
        finally
        {
            _operationGate.Release();
        }
    }

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
                await ReleaseAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _operationGate.Dispose();
        }
    }

    private async Task SendTokenTtlAsync(ushort messageType, ulong ttlSecs, string operation, CancellationToken ct)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteString(string.Empty);
        writer.WriteU64(Token);
        writer.WriteU64(ttlSecs);
        var response = await _request(messageType, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = LeaseWireHelpers.ReadSuccess(response, operation);
        if (reader.RemainingBytes >= 8)
        {
            Token = reader.ReadU64();
            if (!reader.IsEof)
            {
                throw new LeaseException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
            }
        }
        else
        {
            throw new LeaseException($"{operation} response missing fencing token", $"{operation}_INVALID_RESPONSE");
        }
    }

    private void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) == 0)
        {
            return;
        }

        throw new LeaseException("Lease handle is no longer valid after disconnect", "CLOSED");
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
