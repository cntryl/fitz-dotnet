using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

/// <summary>
/// Covers the Wire 3 request correlation header, HELLO negotiation,
/// and the concurrency regimes either side of it.
/// </summary>
public sealed class CorrelationTests
{
    [Fact]
    public void ShouldEncodeHeaderCorrelationGivenRequestWhenEncoding()
    {
        // Arrange
        var payload = new byte[] { 0xAA, 0xBB };

        // Act
        var frame = FrameCodec.EncodeCorrelated(0x0102030405060708, MessageTypes.KvGet, payload);

        // Assert
        var decoded = FrameCodec.DecodeStrict(frame);
        Assert.Equal(MessageTypes.KvGet, decoded.MessageType);
        Assert.Equal(0x0102030405060708UL, decoded.CorrelationId);
        Assert.Equal(payload, decoded.Payload.ToArray());
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
    public void ShouldReadCorrelatedFrameGivenOneTransportFrameWhenParsing()
    {
        // Arrange
        var frame = FrameCodec.EncodeCorrelated(42, MessageTypes.KvGet, [0x7]);

        // Act
        var response = FrameCodec.DecodeStrict(frame);

        // Assert
        Assert.Equal(42UL, response.CorrelationId);
        Assert.Equal(MessageTypes.KvGet, response.MessageType);
        Assert.Equal([0x7], response.Payload.ToArray());
    }

    [Fact]
    public void ShouldAcceptResponseFinalFlagGivenCorrelatedTerminalResponse()
    {
        // Arrange
        var frame = FrameCodec.Encode(MessageTypes.KvGet, [0x6F, 0x6B], flags: FrameCodec.ResponseFinalFlag, correlationId: 1);

        // Act
        var decoded = FrameCodec.DecodeStrict(frame);

        // Assert
        Assert.Equal(FrameCodec.ResponseFinalFlag, decoded.Flags);
        Assert.Equal(1UL, decoded.CorrelationId);
        Assert.Equal(new byte[] { 0x6F, 0x6B }, decoded.Payload.ToArray());
    }

    [Fact]
    public void ShouldCarryMaximumPayloadGivenCorrelatedExtendedFrameWhenParsing()
    {
        // Arrange
        var payload = new byte[FrameCodec.MaxTransportFrameSize - FrameCodec.MaxHeaderSize];
        var frame = FrameCodec.EncodeCorrelated(42, ushort.MaxValue, payload);

        // Act
        var request = FrameCodec.DecodeStrict(frame);

        // Assert
        Assert.Equal(FrameCodec.MaxTransportFrameSize, frame.Length);
        Assert.Equal(42UL, request.CorrelationId);
        Assert.Equal(ushort.MaxValue, request.MessageType);
        Assert.Equal(payload.Length, request.Payload.Length);
    }

    [Fact]
    public void ShouldRejectTrailingCoreBytesGivenHelloWhenParsing()
    {
        // Arrange
        var core = ExtensionCodec.Decode(FrameCodec.DecodeStrict(ConnectionNegotiationTests.HelloFrame())).Core.ToArray();
        var payload = core.Concat(new byte[] { 0, 0 }).ToArray();
        // Act
        var parsed = ServerCapabilities.TryParse(payload, out _);
        // Assert
        Assert.False(parsed);
    }

    [Fact]
    public void ShouldRejectHelloGivenTruncatedPayloadWhenParsing()
    {
        // Arrange
        var truncated = new byte[] { 0x00, 0x01 };

        // Act
        var parsed = ServerCapabilities.TryParse(truncated, out var capabilities);

        // Assert
        Assert.False(parsed);
        Assert.Same(ServerCapabilities.None, capabilities);
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
        mux.DispatchCorrelated(222, new byte[] { 0xB });
        mux.DispatchCorrelated(111, new byte[] { 0xA });

        // Assert: each caller got its own response, not the other's.
        Assert.Equal([0xA], await first.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal([0xB], await second.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task ShouldWaitGivenIntermediateFrameForFinalCorrelatedResponse()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var request = mux.RequestAsync(
            MessageTypes.LeaseAcquire,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 17);

        // Act
        mux.DispatchCorrelated(17, new byte[] { 0, 2 }, responseFinal: false);
        Assert.False(request.IsCompleted);
        mux.DispatchCorrelated(17, new byte[] { 0, 0, 0, 9 }, responseFinal: true);

        // Assert
        Assert.Equal(new byte[] { 0, 0, 0, 9 }, await request.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public void ShouldRejectGivenUnknownCorrelationWhenDispatched()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        // Act
        Action act = () => mux.DispatchCorrelated(99, new byte[] { 0x1 });

        // Assert
        Assert.Throws<ProtocolException>(act);
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
            mux.DispatchCorrelated((ulong)i, new byte[] { (byte)i });
            routed++;
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
            payload => notified.TrySetResult(payload.ToArray()));

        var request = mux.RequestAsync(
            MessageTypes.KvGet,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 77);

        // Act: an uncorrelated notification, then the correlated response.
        mux.DispatchNotification(MessageTypes.KvNotify, new byte[] { 0xFE });

        // Assert
        Assert.Equal([0xFE], await notified.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.False(request.IsCompleted);

        mux.DispatchCorrelated(77, new byte[] { 0xAB });
        Assert.Equal([0xAB], await request.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public void ShouldNotFallbackGivenUnknownCorrelationWhenDispatched()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var called = false;
        using var registration = mux.RegisterNotificationHandler(
            MessageTypes.LeaseAcquire,
            _ => called = true);

        // Act
        Action act = () => mux.DispatchCorrelated(9999, new byte[] { 0x1 });

        // Assert
        Assert.Throws<ProtocolException>(act);
        Assert.False(called);
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
        mux.DispatchCorrelated(5, new byte[] { 0xA });
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
