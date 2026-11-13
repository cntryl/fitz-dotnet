using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// In-memory Wire 3 transport: answers CONNECT with HELLO and every request with a canned final
/// response on the same correlation. Removes network cost so benchmarks measure only client work.
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

    public Task ConnectAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        var remaining = data.Span;
        while (FrameCodec.TryReadHeader(remaining, out var messageType, out _, out var correlationId, out var payloadLength))
        {
            remaining = remaining[(FrameCodec.MaxHeaderSize + (int)payloadLength)..];
            if (messageType == MessageTypes.Connect)
            {
                _inbound.Writer.TryWrite(HelloFrame());
                continue;
            }

            if (correlationId == 0)
            {
                // Fire-and-forget frames get no response.
                continue;
            }

            var response = FrameCodec.Encode(messageType, _responsePayload, FrameCodec.ResponseFinalFlag, correlationId);
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

    static byte[] HelloFrame()
    {
        var core = new byte[22];
        BinaryPrimitives.WriteUInt16BigEndian(core, 3);
        BinaryPrimitives.WriteUInt32BigEndian(core.AsSpan(6), FrameCodec.MaxTransportFrameSize);
        BinaryPrimitives.WriteUInt32BigEndian(core.AsSpan(10), 1024);
        BinaryPrimitives.WriteUInt64BigEndian(core.AsSpan(14), 1);
        using var capabilities = new BinaryBufferWriter();
        capabilities.WriteU16(2);
        capabilities.WriteU32(1);
        capabilities.WriteU32(2);
        return FrameCodec.Encode(
            MessageTypes.Hello,
            ExtensionCodec.Encode(core, 1, capabilities.WrittenMemory.Span),
            ExtensionCodec.PresentFlag);
    }

    async Task RespondAfterLatencyAsync(byte[] response)
    {
        await Task.Delay(_roundTripLatency).ConfigureAwait(false);
        _inbound.Writer.TryWrite(response);
    }

    public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken ct = default)
    {
        var frame = await _inbound.Reader.ReadAsync(ct).ConfigureAwait(false);
        var rented = ArrayPool<byte>.Shared.Rent(frame.Length);
        frame.CopyTo(rented, 0);
        return PooledFrame.FromRentedBuffer(rented, frame.Length);
    }

    public Task CloseAsync(CancellationToken ct = default)
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
