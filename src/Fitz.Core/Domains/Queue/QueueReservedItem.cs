using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Queue;

sealed class QueueReservedItem : QueueItem
{
    readonly ulong _id;
    readonly ulong _token;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _requestFn;
    readonly SessionLatch _session;
    bool _completing;
    readonly Func<uint, bool>? _supportsNack;

    internal QueueReservedItem(
        string route,
        ReadOnlyMemory<byte> body,
        ulong id,
        ulong token,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> requestFn,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        Func<uint, bool>? supportsNack = null)
        : base(route, body)
    {
        _id = id;
        _token = token;
        _requestFn = requestFn;
        _supportsNack = supportsNack;
        _session = new SessionLatch(registerOnDisconnect);
    }

    public override async Task ExtendAsync(TimeSpan lease, CancellationToken ct = default)
    {
        var leaseSeconds = WireDuration.ToSeconds(lease, nameof(lease));
        ArgumentOutOfRangeException.ThrowIfZero(leaseSeconds, nameof(lease));
        ThrowIfClosed();

        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteU64(_id);
        writer.WriteU64(_token);
        writer.WriteU64(leaseSeconds);

        var response = await _requestFn(MessageTypes.QueueExtend, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = QueueWireHelpers.ReadSuccess(response, "EXTEND");

        if (!reader.IsEof)
        {
            throw new QueueException("EXTEND response has trailing bytes", "EXTEND_INVALID_RESPONSE");
        }
    }

    public override Task CompleteAsync(CancellationToken ct = default) => CompleteCoreAsync(_token, ct);

    public override Task CompleteWithTokenAsync(ulong token, CancellationToken ct = default) => CompleteCoreAsync(token, ct);

    public override async Task NackAsync(TimeSpan visibilityDelay = default, CancellationToken ct = default)
    {
        ThrowIfClosed();
        var seconds = WireDuration.ToSeconds(visibilityDelay, nameof(visibilityDelay));
        if (_supportsNack?.Invoke(2) != true)
            throw new NotSupportedException("Queue NACK capability was not negotiated.");
        BeginCompletion();
        try
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteString(Route);
            writer.WriteU64(_id);
            writer.WriteU64(_token);
            writer.WriteU64(seconds);
            var response = await _requestFn(MessageTypes.QueueNack, writer.WrittenMemory, ct).ConfigureAwait(false);
            var reader = QueueWireHelpers.ReadSuccess(response, "NACK");
            if (!reader.IsEof)
                throw new QueueException("NACK response has trailing bytes", "NACK_INVALID_RESPONSE");
            MarkClosed();
        }
        finally
        {
            Volatile.Write(ref _completing, false);
        }
    }

    public override ValueTask DisposeAsync()
    {
        MarkClosed();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    async Task CompleteCoreAsync(ulong token, CancellationToken ct)
    {
        BeginCompletion();
        try
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteString(Route);
            writer.WriteU64(_id);
            writer.WriteU64(token);

            var response = await _requestFn(MessageTypes.QueueComplete, writer.WrittenMemory, ct).ConfigureAwait(false);
            var reader = QueueWireHelpers.ReadSuccess(response, "COMPLETE");

            if (!reader.IsEof)
            {
                throw new QueueException("COMPLETE response has trailing bytes", "COMPLETE_INVALID_RESPONSE");
            }

            MarkClosed();
        }
        finally
        {
            Volatile.Write(ref _completing, false);
        }
    }

    void ThrowIfClosed()
    {
        if (!_session.IsClosed && !Volatile.Read(ref _completing))
        {
            return;
        }

        throw new QueueException("Queue item is no longer valid after disconnect", "ITEM_CLOSED");
    }

    void MarkClosed() => _session.TryClose();

    void BeginCompletion()
    {
        if (!_session.IsClosed && !Interlocked.CompareExchange(ref _completing, true, false))
        {
            return;
        }

        throw new QueueException("Queue item is completing or closed", "ITEM_CLOSED");
    }
}
