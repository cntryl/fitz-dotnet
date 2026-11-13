using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Rpc;

// Closed when its invocation ends: a send the handler left running finishes first, and a
// send that starts afterwards fails, because the response stream no longer exists.
sealed class RpcResponseWriter : IRpcResponseWriter, IAsyncDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    readonly byte[] _correlationId;
    readonly CancellationToken _connectionClosed;
    readonly SemaphoreSlim _sendGate = new(1, 1);
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    ulong _sequence;
    bool _ended;
    int _disposed;

    internal bool IsEnded => _ended;

    internal RpcResponseWriter(Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send, byte[] correlationId,
        CancellationToken connectionClosed)
    {
        _send = send;
        _correlationId = correlationId;
        _connectionClosed = connectionClosed;
        _closingToken = _closing.Token;
    }

    public async ValueTask SendAsync(ReadOnlyMemory<byte> body, bool isEnd = false, CancellationToken ct = default)
    {
        using var linked = _connectionClosed.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, _connectionClosed) : null;
        var sendToken = linked?.Token ?? ct;
        await _sendGate.EnterAsync(this, _closingToken, sendToken).ConfigureAwait(false);
        try
        {
            if (_ended)
            {
                throw new InvalidOperationException("The RPC response stream has already ended.");
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteBytes(_correlationId);
            writer.WriteU64(_sequence);
            writer.WriteU8(isEnd ? RpcWire.ResponseFlagStreamEnd : (byte)0);
            writer.WriteLengthPrefixed(body.Span);

            await _send(MessageTypes.RpcResponse, writer.WrittenMemory, sendToken).ConfigureAwait(false);
            _sequence++;
            _ended = isEnd;
        }
        finally
        {
            _sendGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _closing.CancelAsync().ConfigureAwait(false);
        await _sendGate.DrainAndDisposeAsync().ConfigureAwait(false);
        _closing.Dispose();
    }
}
