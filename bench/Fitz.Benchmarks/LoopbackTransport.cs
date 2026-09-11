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
    readonly bool _correlationEnabled;

    public LoopbackTransport(
        int responsePayloadSize = 64,
        TimeSpan roundTripLatency = default,
        bool correlationEnabled = false)
    {
        _responsePayload = new byte[responsePayloadSize];
        _roundTripLatency = roundTripLatency;
        _correlationEnabled = correlationEnabled;
    }

    public Uri Url { get; } = new("ws://loopback/ws");

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_correlationEnabled)
        {
            var hello = new byte[6];
            System.Buffers.Binary.BinaryPrimitives.WriteUInt16BigEndian(hello.AsSpan(0, 2), 1);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(
                hello.AsSpan(2, 4),
                ServerCapabilities.CorrelationBit);
            _inbound.Writer.TryWrite(FrameCodec.Encode(MessageTypes.ServerHello, hello));
        }

        return Task.CompletedTask;
    }

    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var span = data.Span;
        var offset = 0;
        ulong pendingCorrelation = 0;
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
            var payloadStart = offset + headerLength;
            offset = payloadStart + payloadLength;

            // A CORRELATE record labels the request in the next record of this same frame.
            if (messageType == MessageTypes.Correlate)
            {
                pendingCorrelation = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(
                    span.Slice(payloadStart, payloadLength));
                continue;
            }

            if (messageType == MessageTypes.Connect)
            {
                pendingCorrelation = 0;
                continue;
            }

            var response = pendingCorrelation == 0
                ? FrameCodec.Encode(messageType, _responsePayload)
                : BuildCorrelatedResponse(pendingCorrelation, messageType);
            pendingCorrelation = 0;
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

    byte[] BuildCorrelatedResponse(ulong correlationId, ushort messageType)
    {
        var frame = FrameCodec.EncodeCorrelated(correlationId, messageType, _responsePayload);
        frame[0] = (byte)MessageTypes.Correlated;
        return frame;
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
