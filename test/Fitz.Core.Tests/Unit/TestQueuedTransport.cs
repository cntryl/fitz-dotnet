using System.Buffers;
using System.Threading.Channels;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

class TestQueuedTransport : ITransport
{
    readonly Channel<PooledFrame> _incoming = Channel.CreateUnbounded<PooledFrame>();
    readonly object _sentFramesGate = new();
    readonly List<(ushort Type, byte[] Payload, byte Flags)> _responses = [];
    readonly HashSet<ulong> _answered = [];

    public List<byte[]> SentFrames { get; } = [];

    public Action<int>? AfterSend { get; set; }

    public Func<Task>? BeforeReceiveReturns { get; set; }

    public bool AutoHello { get; set; } = true;

    public virtual Uri Url { get; } = new("ws://queued");

    public virtual Task ConnectAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        int sentFrameCount;
        lock (_sentFramesGate)
        {
            SentFrames.Add(data.ToArray());
            sentFrameCount = SentFrames.Count;
        }

        if (AutoHello && FrameCodec.DecodeStrict(data).MessageType == MessageTypes.Connect)
        {
            QueueIncomingFrame(ConnectionNegotiationTests.HelloFrame());
        }
        var sent = FrameCodec.DecodeStrict(data);
        lock (_sentFramesGate)
        {
            var index = _responses.FindIndex(response => response.Type == sent.MessageType);
            if (index >= 0)
            {
                var response = _responses[index];
                _responses.RemoveAt(index);
                QueueResponse(response.Type, response.Payload, response.Flags, sent.CorrelationId);
            }
        }
        AfterSend?.Invoke(sentFrameCount);
        return Task.CompletedTask;
    }

    public ValueTask<PooledFrame> ReceiveAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return ReceiveCoreAsync(ct);
    }

    async ValueTask<PooledFrame> ReceiveCoreAsync(CancellationToken ct)
    {
        var frame = await _incoming.Reader.ReadAsync(ct).ConfigureAwait(false);
        if (BeforeReceiveReturns is { } beforeReceiveReturns)
        {
            await beforeReceiveReturns().ConfigureAwait(false);
        }

        return frame;
    }

    public void QueueIncomingFrame(byte[] frame)
    {
        ArgumentNullException.ThrowIfNull(frame);

        var buffer = ArrayPool<byte>.Shared.Rent(frame.Length);
        frame.AsSpan().CopyTo(buffer);
        _incoming.Writer.TryWrite(PooledFrame.FromRentedBuffer(buffer, frame.Length));
    }

    public void QueueResponse(ushort messageType, ReadOnlySpan<byte> payload, byte flags = 0, ulong correlationId = 0)
    {
        if (correlationId == 0 && messageType is not (1 or 2 or 3 or 4 or 111 or 209 or 302 or 303 or 304 or 305 or 409 or 504 or 609 or 705))
        {
            lock (_sentFramesGate)
            {
                var request = SentFrames.Select(frame => FrameCodec.DecodeStrict(frame))
                    .FirstOrDefault(frame => frame.MessageType == messageType && !_answered.Contains(frame.CorrelationId));
                if (request.CorrelationId == 0)
                {
                    _responses.Add((messageType, payload.ToArray(), flags));
                    return;
                }
                correlationId = request.CorrelationId;
            }
        }
        if (messageType != MessageTypes.LeaseAcquire || !payload.StartsWith(new byte[] { 0, 2 }))
        {
            lock (_sentFramesGate)
            {
                _answered.Add(correlationId);
            }
        }
        if (correlationId != 0 && !(messageType == 400 && payload.StartsWith(new byte[] { 0, 2 })))
        {
            flags |= FrameCodec.ResponseFinalFlag;
        }
        QueueIncomingFrame(FrameCodec.Encode(messageType, payload, flags, correlationId));
    }

    public void QueueClosed() => _incoming.Writer.TryWrite(PooledFrame.Closed);

    public Task CloseAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _incoming.Writer.TryComplete();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _incoming.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
