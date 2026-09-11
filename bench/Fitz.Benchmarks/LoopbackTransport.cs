using System.Buffers;
using System.Threading.Channels;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// In-memory transport that echoes a canned response for every request frame.
/// Removes network cost so benchmarks measure only client-side work.
/// </summary>
public sealed class LoopbackTransport : ITransport
{
    readonly Channel<byte[]> _inbound = Channel.CreateUnbounded<byte[]>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

    readonly byte[] _responsePayload;
    readonly TimeSpan _roundTripLatency;

    public LoopbackTransport(int responsePayloadSize = 64, TimeSpan roundTripLatency = default)
    {
        _responsePayload = new byte[responsePayloadSize];
        _roundTripLatency = roundTripLatency;
    }

    public Uri Url { get; } = new("ws://loopback/ws");

    public Task ConnectAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var span = data.Span;
        var offset = 0;
        while (offset + 3 <= span.Length)
        {
            ushort messageType;
            int headerLength;
            if (span[offset] == 0xFF)
            {
                if (offset + 5 > span.Length)
                {
                    break;
                }

                messageType = (ushort)((span[offset + 1] << 8) | span[offset + 2]);
                headerLength = 5;
            }
            else
            {
                messageType = span[offset];
                headerLength = 3;
            }

            var payloadLength = (span[offset + headerLength - 2] << 8) | span[offset + headerLength - 1];
            offset += headerLength + payloadLength;
            if (messageType == MessageTypes.Connect)
            {
                continue;
            }

            var response = FrameCodec.Encode(messageType, _responsePayload);
            if (_roundTripLatency <= TimeSpan.Zero)
            {
                _inbound.Writer.TryWrite(response);
            }
            else
            {
                _ = RespondAfterLatencyAsync(response);
            }
        }

        return Task.CompletedTask;
    }

    async Task RespondAfterLatencyAsync(byte[] response)
    {
        await Task.Delay(_roundTripLatency).ConfigureAwait(false);
        _inbound.Writer.TryWrite(response);
    }

    public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var frame = await _inbound.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        var rented = ArrayPool<byte>.Shared.Rent(frame.Length);
        frame.CopyTo(rented, 0);
        return PooledFrame.FromRentedBuffer(rented, frame.Length);
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        _inbound.Writer.TryComplete();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _inbound.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
