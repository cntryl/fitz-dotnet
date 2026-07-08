using System.Buffers;
using System.Threading.Channels;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

internal sealed class TestQueuedTransport : ITransport
{
    private readonly Channel<PooledFrame> _incoming = Channel.CreateUnbounded<PooledFrame>();
    private readonly object _sentFramesGate = new();

    public List<byte[]> SentFrames { get; } = [];

    public Action<int>? AfterSend { get; set; }

    public string Url => "ws://queued";

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        int sentFrameCount;
        lock (_sentFramesGate)
        {
            SentFrames.Add(data.ToArray());
            sentFrameCount = SentFrames.Count;
        }

        AfterSend?.Invoke(sentFrameCount);
        return Task.CompletedTask;
    }

    public ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _incoming.Reader.ReadAsync(cancellationToken);
    }

    public void QueueIncomingFrame(byte[] frame)
    {
        ArgumentNullException.ThrowIfNull(frame);

        var buffer = ArrayPool<byte>.Shared.Rent(frame.Length);
        frame.AsSpan().CopyTo(buffer);
        _incoming.Writer.TryWrite(PooledFrame.FromRentedBuffer(buffer, frame.Length));
    }

    public void QueueClosed()
    {
        _incoming.Writer.TryWrite(PooledFrame.Closed);
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _incoming.Writer.TryComplete();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _incoming.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
