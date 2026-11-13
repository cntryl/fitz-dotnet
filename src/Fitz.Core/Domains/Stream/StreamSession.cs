using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Stream;

/// <summary>
/// The default <see cref="IStreamSession"/>. Obtained from <see cref="StreamClient"/>.
/// </summary>
sealed class StreamSession : IStreamSession
{
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly ulong _sessionId;
    readonly TimeProvider _time;
    readonly SessionLatch _session;
    readonly SemaphoreSlim _finalizationGate = new(1, 1);
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    bool _disposed;

    internal StreamSession(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        ulong sessionId,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        TimeProvider? timeProvider = null)
    {
        _request = request;
        _sessionId = sessionId;
        _time = timeProvider ?? TimeProvider.System;
        _closingToken = _closing.Token;
        _session = new SessionLatch(registerOnDisconnect);
    }

    /// <inheritdoc />
    public async Task<ulong?> AppendAsync(ulong expectedOffset, ReadOnlyMemory<byte> body, ReadOnlyMemory<byte>? metadata = null, string? discriminator = null, CancellationToken ct = default)
    {
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        writer.WriteU64(expectedOffset);
        writer.WriteLengthPrefixed(body.Span);
        writer.WriteOptionalLengthPrefixed(metadata);

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

    /// <inheritdoc />
    public async Task CommitAsync(StreamCommitMode mode, CancellationToken ct = default)
    {
        var wireMode = mode switch
        {
            StreamCommitMode.Buffered => (byte)0,
            StreamCommitMode.Sync => (byte)1,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown stream commit mode."),
        };

        if (!await _finalizationGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            throw ClosedException();

        try
        {
            ThrowIfClosed();
            using var writer = new BinaryBufferWriter();
            writer.WriteU64(_sessionId);
            writer.WriteU8(wireMode);
            await ExpectStatusAsync(MessageTypes.StreamCommit, writer.WrittenMemory, "COMMIT", ct).ConfigureAwait(false);
        }
        finally
        {
            _finalizationGate.Release();
        }
    }

    /// <inheritdoc />
    public async Task RollbackAsync(CancellationToken ct = default)
    {
        if (!await _finalizationGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            return;

        try
        {
            await RollbackCoreAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _finalizationGate.Release();
        }
    }

    /// <summary>Rolls the session back if it has not been committed.</summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    /// <remarks>
    /// Waits for an in-progress commit or rollback to finish first. A queued commit fails as
    /// closed; a queued rollback returns, because disposal rolls back.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, true))
        {
            return;
        }

        await _closing.CancelAsync().ConfigureAwait(false);
        await _finalizationGate.DrainAndDisposeAsync().ConfigureAwait(false);
        _closing.Dispose();
        try
        {
            using var cleanupDeadline = new CancellationTokenSource(CleanupBudget, _time);
            await RollbackCoreAsync(cleanupDeadline.Token).ConfigureAwait(false);
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

    // Callers hold the gate, or have drained it during disposal.
    async Task RollbackCoreAsync(CancellationToken ct)
    {
        if (_session.IsClosed)
        {
            return;
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(_sessionId);
        await ExpectStatusAsync(MessageTypes.StreamRollback, writer.WrittenMemory, "ROLLBACK", ct).ConfigureAwait(false);
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
        if (_session.IsClosed)
            throw ClosedException();
    }

    static StreamException ClosedException() => new("Stream session already closed", "SESSION_CLOSED");

    void MarkClosed() => _session.TryClose();
}
