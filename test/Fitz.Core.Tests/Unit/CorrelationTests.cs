using System.Buffers.Binary;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

/// <summary>
/// Covers the request-correlation contract: CORRELATE/CORRELATED framing, capability negotiation,
/// and the concurrency regimes either side of it.
/// </summary>
public sealed class CorrelationTests
{
    [Fact]
    public void ShouldEncodeTwoRecordsGivenCorrelatedRequestWhenEncoding()
    {
        // Arrange
        var payload = new byte[] { 0xAA, 0xBB };

        // Act
        var frame = FrameCodec.EncodeCorrelated(0x0102030405060708, MessageTypes.KvGet, payload);

        // Assert
        Assert.Equal(MessageTypes.Correlate, frame[0]);
        Assert.Equal(8, BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(1, 2)));
        Assert.Equal(0x0102030405060708UL, BinaryPrimitives.ReadUInt64BigEndian(frame.AsSpan(3, 8)));

        // The labelled request follows immediately, in the same transport frame.
        Assert.Equal(MessageTypes.KvGet, frame[FrameCodec.CorrelationRecordSize]);
        Assert.Equal(payload.Length, BinaryPrimitives.ReadUInt16BigEndian(frame.AsSpan(FrameCodec.CorrelationRecordSize + 1, 2)));
        Assert.Equal(payload, frame.AsSpan(FrameCodec.CorrelationRecordSize + 3).ToArray());
    }

    [Fact]
    public void ShouldRejectGivenZeroIdentifierWhenEncodingCorrelatedRequest()
    {
        // Arrange
        var payload = new byte[] { 0x1 };

        // Act
        var act = () => FrameCodec.EncodeCorrelated(0, MessageTypes.KvGet, payload);

        // Assert
        Assert.Throws<ProtocolException>(act);
    }

    [Fact]
    public void ShouldReadBothRecordsGivenOneTransportFrameWhenParsing()
    {
        // Arrange: a correlated response is two TLV records in a single transport frame.
        var parser = new FrameParser();
        var frame = FrameCodec.EncodeCorrelated(42, MessageTypes.KvGet, [0x7]);
        frame[0] = (byte)MessageTypes.Correlated;

        // Act
        parser.Append(frame);

        // Assert
        Assert.True(parser.TryReadFrame(out var label));
        Assert.Equal(MessageTypes.Correlated, label.MessageType);
        Assert.Equal(42UL, FrameCodec.ReadCorrelationId(label.Payload.Span));

        Assert.True(parser.TryReadFrame(out var response));
        Assert.Equal(MessageTypes.KvGet, response.MessageType);
        Assert.Equal([0x7], response.Payload.ToArray());

        Assert.False(parser.TryReadFrame(out _));
    }

    [Fact]
    public void ShouldCarryMaximumPayloadGivenCorrelatedExtendedFrameWhenParsing()
    {
        // Arrange
        var payload = new byte[ushort.MaxValue];
        var frame = FrameCodec.EncodeCorrelated(42, ushort.MaxValue, payload);
        var parser = new FrameParser();

        // Act
        parser.Append(frame);

        // Assert
        Assert.Equal(FrameCodec.MaxTransportFrameSize, frame.Length);
        Assert.True(parser.TryReadFrame(out var label));
        Assert.Equal(MessageTypes.Correlate, label.MessageType);
        Assert.True(parser.TryReadFrame(out var request));
        Assert.Equal(ushort.MaxValue, request.MessageType);
        Assert.Equal(ushort.MaxValue, request.Payload.Length);
        Assert.False(parser.TryReadFrame(out _));
    }

    [Fact]
    public void ShouldParseGivenServerHelloWithTrailingBytesWhenAdvertising()
    {
        // Arrange: a later protocol version may append fields; readers ignore the tail.
        Span<byte> payload = stackalloc byte[10];
        BinaryPrimitives.WriteUInt16BigEndian(payload[..2], 3);
        BinaryPrimitives.WriteUInt32BigEndian(payload.Slice(2, 4), ServerCapabilities.CorrelationBit);

        // Act
        var parsed = ServerCapabilities.TryParse(payload, out var capabilities);

        // Assert
        Assert.True(parsed);
        Assert.Equal(3, capabilities.ProtocolVersion);
        Assert.True(capabilities.SupportsCorrelation);
    }

    [Fact]
    public void ShouldReportNoCorrelationGivenTruncatedServerHelloWhenAdvertising()
    {
        // Arrange
        var truncated = new byte[] { 0x00, 0x01 };

        // Act
        var parsed = ServerCapabilities.TryParse(truncated, out var capabilities);

        // Assert: an unparseable advertisement is no more disruptive than a missing one.
        Assert.False(parsed);
        Assert.False(capabilities.SupportsCorrelation);
    }

    [Fact]
    public async Task ShouldResolveOwnCallerGivenOutOfOrderResponsesWhenCorrelated()
    {
        // Arrange: CS-019. Two same-message-type requests; the broker answers the second first.
        using var mux = new Multiplexer();
        mux.SetConnected();

        var first = mux.RequestAsync(
            MessageTypes.QueueReserve,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 111);
        var second = mux.RequestAsync(
            MessageTypes.QueueReserve,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 222);

        // Act: answer out of receive order, exactly as a parked RESERVE would.
        Assert.True(mux.DispatchCorrelated(222, MessageTypes.QueueReserve, new byte[] { 0xB }));
        Assert.True(mux.DispatchCorrelated(111, MessageTypes.QueueReserve, new byte[] { 0xA }));

        // Assert: each caller got its own response, not the other's.
        Assert.Equal([0xA], await first.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal([0xB], await second.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task ShouldNotSerializeGivenManySameTypeRequestsWhenCorrelated()
    {
        // Arrange: correlated requests share no lane, so all of them reach the transport.
        using var mux = new Multiplexer();
        mux.SetConnected();
        var sent = 0;

        var requests = new List<Task<byte[]>>();
        for (var i = 1; i <= 32; i++)
        {
            requests.Add(mux.RequestAsync(
                MessageTypes.KvGet,
                [(byte)i],
                (_, _) =>
                {
                    Interlocked.Increment(ref sent);
                    return Task.CompletedTask;
                },
                TimeSpan.FromSeconds(10),
                correlationId: (ulong)i));
        }

        var sentBeforeAnyResponse = Volatile.Read(ref sent);

        // Act: answer every one of them in reverse order.
        var routed = 0;
        for (var i = 32; i >= 1; i--)
        {
            if (mux.DispatchCorrelated((ulong)i, MessageTypes.KvGet, new byte[] { (byte)i }))
            {
                routed++;
            }
        }

        // Assert: all 32 reached the transport before any answer. Uncorrelated, only one would have.
        Assert.Equal(32, sentBeforeAnyResponse);
        Assert.Equal(32, routed);
        for (var i = 0; i < requests.Count; i++)
        {
            Assert.Equal([(byte)(i + 1)], await requests[i].WaitAsync(TimeSpan.FromSeconds(10)));
        }
    }

    [Fact]
    public async Task ShouldNotConsumeCorrelatedRequestGivenNotificationWhenBothInFlight()
    {
        // Arrange: CS-022. A NOTIFY arriving amid correlated requests must reach its subscriber and
        // must not be taken as any request's response.
        using var mux = new Multiplexer();
        mux.SetConnected();
        var notified = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = mux.RegisterNotificationHandler(
            MessageTypes.KvNotify,
            payload => notified.TrySetResult(payload));

        var request = mux.RequestAsync(
            MessageTypes.KvGet,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 77);

        // Act: an uncorrelated notification, then the correlated response.
        mux.Dispatch(MessageTypes.KvNotify, new byte[] { 0xFE });

        // Assert
        Assert.Equal([0xFE], await notified.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.False(request.IsCompleted);

        Assert.True(mux.DispatchCorrelated(77, MessageTypes.KvGet, new byte[] { 0xAB }));
        Assert.Equal([0xAB], await request.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public void ShouldDropGivenCorrelatedResponseForAbandonedRequestWhenDispatched()
    {
        // Arrange: CS-021's converse. Nobody is waiting on this identifier.
        using var mux = new Multiplexer();
        mux.SetConnected();

        // Act
        var routed = mux.DispatchCorrelated(9999, MessageTypes.KvGet, new byte[] { 0x1 });

        // Assert: dropped, never handed to an unrelated waiter.
        Assert.False(routed);
    }

    [Fact]
    public async Task ShouldRejectGivenIdentifierAlreadyInFlightWhenRequesting()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var inFlight = mux.RequestAsync(
            MessageTypes.KvGet,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 5);

        // Act
        var act = () => mux.RequestAsync(
            MessageTypes.KvGet,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 5);

        // Assert
        await Assert.ThrowsAsync<ProtocolException>(act);
        Assert.True(mux.DispatchCorrelated(5, MessageTypes.KvGet, new byte[] { 0xA }));
        Assert.Equal([0xA], await inFlight.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task ShouldFailCorrelatedRequestsGivenConnectionLossWhenInFlight()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var request = mux.RequestAsync(
            MessageTypes.KvGet,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 31);

        // Act
        mux.SetDisconnected();

        // Assert: a correlated request must not be stranded by a session reset.
        await Assert.ThrowsAsync<ConnectionException>(() => request);
    }
}
