using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

/// <summary>Wire-2 negotiation and correlated routing through the real receive loop.</summary>
public sealed class CorrelationConnectionTests
{
    static ClientConfig Config(TimeSpan? timeout = null) => new(
        new Uri("ws://queued/ws"),
        Retry: new RetryOptions(Enabled: false),
        Reconnect: new ReconnectOptions(Enabled: false),
        Timeout: timeout ?? TimeSpan.FromSeconds(10));

    static async Task WaitForAsync(Func<bool> condition, string because)
    {
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(5);
        }
        Assert.Fail(because);
    }

    [Fact]
    public void ShouldValidateServiceNameGivenUtf8ByteLimitWhenConfigIsValidated()
    {
        // Arrange
        var valid = Config() with { ServiceName = string.Concat(Enumerable.Repeat("é", 64)) };
        var oversized = Config() with { ServiceName = string.Concat(Enumerable.Repeat("é", 65)) };
        // Act
        valid.Validate();
        // Assert
        Assert.Throws<ArgumentException>(() => oversized.Validate());
    }

    [Fact]
    public async Task ShouldEnableCorrelationGivenWire2HelloWithoutOptionalBitsWhenConnected()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        // Act
        await connection.ConnectAsync();
        // Assert
        Assert.True(connection.CorrelationEnabled);
        Assert.Equal(2, connection.Capabilities.ProtocolVersion);
        Assert.Equal(0u, connection.Capabilities.CapabilityBits);
    }

    [Fact]
    public async Task ShouldCarryServiceNameGivenConfigurationWhenConnectIsSent()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config() with { ServiceName = " orders-worker " }, () => transport);
        // Act
        await connection.ConnectAsync();
        var frame = FrameCodec.DecodeStrict(Assert.Single(transport.SentFrames));
        var reader = new BinaryBufferReader(frame.Payload.ToArray());
        // Assert
        Assert.Equal(MessageTypes.Connect, frame.MessageType);
        Assert.Equal(0UL, frame.CorrelationId);
        Assert.Equal(2, reader.ReadU16());
        Assert.Equal(2, reader.ReadU16());
        Assert.Equal(0u, reader.ReadU32());
        Assert.Equal(string.Empty, reader.ReadString());
        Assert.Equal("orders-worker", reader.ReadString());
    }

    [Fact]
    public async Task ShouldFailConnectGivenNoHelloWhenDeadlineExpires()
    {
        // Arrange
        await using var transport = new TestQueuedTransport { AutoHello = false };
        await using var connection = new FitzConnection(Config(TimeSpan.FromMilliseconds(100)), () => transport);
        // Act
        var failure = await Assert.ThrowsAsync<RequestTimeoutException>(() => connection.ConnectAsync());
        // Assert
        Assert.Contains("HELLO", failure.Message, StringComparison.Ordinal);
        Assert.NotEqual(ConnectionState.Authenticated, connection.State);
    }

    [Fact]
    public async Task ShouldIgnoreUnknownCapabilityBitsGivenValidWire2HelloWhenConnecting()
    {
        // Arrange
        await using var transport = new TestQueuedTransport { AutoHello = false };
        transport.QueueIncomingFrame(ConnectionNegotiationTests.HelloFrame(0x8000_0000));
        await using var connection = new FitzConnection(Config(), () => transport);
        // Act
        await connection.ConnectAsync();
        // Assert
        Assert.True(connection.CorrelationEnabled);
        Assert.Equal(ConnectionState.Authenticated, connection.State);
    }

    [Fact]
    public async Task ShouldLabelRequestGivenZeroOptionalBitsWhenSending()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();
        // Act
        var request = connection.RequestAsync(MessageTypes.KvGet, new byte[] { 1 }).AsTask();
        await WaitForAsync(() => transport.SentFrames.Count == 2, "Request should reach transport.");
        var sent = FrameCodec.DecodeStrict(transport.SentFrames[1]);
        transport.QueueIncomingFrame(FrameCodec.EncodeCorrelated(sent.CorrelationId, MessageTypes.KvGet, [0xAB]));
        var response = await request.WaitAsync(TimeSpan.FromSeconds(2));
        // Assert
        Assert.Equal(MessageTypes.KvGet, sent.MessageType);
        Assert.NotEqual(0UL, sent.CorrelationId);
        Assert.Equal([0xAB], response.ToArray());
    }

    [Fact]
    public async Task ShouldResolveOwnCallerGivenOutOfOrderRepliesWhenBothRequestsShareAType()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();
        var first = connection.RequestAsync(MessageTypes.QueueReserve, new byte[] { 1 }).AsTask();
        var second = connection.RequestAsync(MessageTypes.QueueReserve, new byte[] { 2 }).AsTask();
        await WaitForAsync(() => transport.SentFrames.Count == 3, "Both requests should reach transport.");
        var firstId = FrameCodec.DecodeStrict(transport.SentFrames[1]).CorrelationId;
        var secondId = FrameCodec.DecodeStrict(transport.SentFrames[2]).CorrelationId;
        // Act
        transport.QueueIncomingFrame(FrameCodec.EncodeCorrelated(secondId, MessageTypes.QueueReserve, [0xB]));
        transport.QueueIncomingFrame(FrameCodec.EncodeCorrelated(firstId, MessageTypes.QueueReserve, [0xA]));
        // Assert
        Assert.NotEqual(firstId, secondId);
        Assert.Equal([0xA], (await first.WaitAsync(TimeSpan.FromSeconds(2))).ToArray());
        Assert.Equal([0xB], (await second.WaitAsync(TimeSpan.FromSeconds(2))).ToArray());
    }

    [Fact]
    public async Task ShouldKeepSessionGivenOneRequestTimesOutWhenNextRequestIsCorrelated()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(TimeSpan.FromMilliseconds(200)), () => transport);
        await connection.ConnectAsync();
        await Assert.ThrowsAsync<RequestTimeoutException>(() => connection.RequestAsync(MessageTypes.KvGet, new byte[] { 1 }).AsTask());
        // Act
        var next = connection.RequestAsync(MessageTypes.KvGet, new byte[] { 2 }).AsTask();
        await WaitForAsync(() => transport.SentFrames.Count == 3, "Next request should use current transport.");
        var id = FrameCodec.DecodeStrict(transport.SentFrames[2]).CorrelationId;
        transport.QueueIncomingFrame(FrameCodec.EncodeCorrelated(id, MessageTypes.KvGet, [0xAB]));
        // Assert
        Assert.Equal([0xAB], (await next.WaitAsync(TimeSpan.FromSeconds(2))).ToArray());
        Assert.Equal(ConnectionState.Authenticated, connection.State);
    }

    [Fact]
    public async Task ShouldRouteLaterPhaseGivenCorrelationNoLongerPendingWhenReceived()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();
        var later = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = connection.RegisterNotificationHandler(MessageTypes.LeaseAcquire, payload => later.TrySetResult(payload));
        // Act
        transport.QueueIncomingFrame(FrameCodec.EncodeCorrelated(999, MessageTypes.LeaseAcquire, [0xCA]));
        // Assert
        Assert.Equal([0xCA], await later.Task.WaitAsync(TimeSpan.FromSeconds(2)));
    }
}
