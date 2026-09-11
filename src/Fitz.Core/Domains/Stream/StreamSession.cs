using System.Threading;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamSession : IStreamSession
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly ulong _sessionId;
    IDisposable? _disconnectRegistration;
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposal may race active operations; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _finalizationGate = new(1, 1);
    int _closed;
    int _disposed;

    internal StreamSession(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        ulong sessionId,
        Func<Action, IDisposable>? registerOnDisconnect = null)
    {
        _request = request;
        _sessionId = sessionId;
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

    public async Task<ulong?> AppendAsync(ulong expectedOffset, ReadOnlyMemory<byte> body, ReadOnlyMemory<byte>? metadata = null, string? discriminator = null, CancellationToken ct = default)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        writer.WriteU64(expectedOffset);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);
        if (metadata.HasValue && metadata.Value.Length > 0)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)metadata.Value.Length);
            writer.WriteBytes(metadata.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }

        if (!string.IsNullOrEmpty(discriminator))
        {
            writer.WriteU8(1);
            writer.WriteString(discriminator);
        }
        else
        {
            writer.WriteU8(0);
        }

        var response = await _request(MessageTypes.StreamAppend, writer.WrittenMemory, ct).ConfigureAwait(false);
        var data = StreamWireHelpers.ReadOptionalPayload(response, "APPEND");
        if (data.IsEmpty)
        {
            return null;
        }

        if (data.Length != 8)
        {
            throw new StreamException("APPEND response has an invalid committed offset", "APPEND_INVALID_RESPONSE");
        }

        var wrapped = new BinaryBufferReader(data);
        var committedOffset = wrapped.ReadU64();
        return committedOffset;
    }

    public async Task CommitAsync(CancellationToken ct = default)
    {
        await _finalizationGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            using var writer = new BinaryBufferWriter();
            writer.WriteU64(_sessionId);
            writer.WriteU8(0);
            await ExpectStatusAsync(MessageTypes.StreamCommit, writer.WrittenMemory, "COMMIT", ct).ConfigureAwait(false);
        }
        finally
        {
            _finalizationGate.Release();
        }
    }

    public async Task RollbackAsync(CancellationToken ct = default)
    {
        await _finalizationGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _closed) != 0)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU64(_sessionId);
            await ExpectStatusAsync(MessageTypes.StreamRollback, writer.WrittenMemory, "ROLLBACK", ct).ConfigureAwait(false);
        }
        finally
        {
            _finalizationGate.Release();
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Stream-session disposal is bounded best-effort cleanup and must not replace an exception leaving an await-using scope.")]
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
                }
            }
        }
        finally
        {
            MarkClosed();
            GC.SuppressFinalize(this);
        }
    }

    async Task ExpectStatusAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        ThrowIfClosed();
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        StreamWireHelpers.EnsureSuccessStatusOnly(response, operation);
        MarkClosed();
    }

    void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) == 0)
        {
            return;
        }

        throw new StreamException("Stream session already closed", "SESSION_CLOSED");
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
