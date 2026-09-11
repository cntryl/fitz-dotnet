using System.Buffers.Binary;
using System.Diagnostics;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

/// <summary>
/// End-to-end correlation behaviour through <see cref="FitzConnection"/>: capability advertisement,
/// legacy fallback, and out-of-order response routing over a real receive loop.
/// </summary>
public sealed class CorrelationConnectionTests
{
    static byte[] ServerHelloFrame(uint capabilityBits, ushort protocolVersion = 1)
    {
        var payload = new byte[6];
        BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(0, 2), protocolVersion);
        BinaryPrimitives.WriteUInt32BigEndian(payload.AsSpan(2, 4), capabilityBits);
        return FrameCodec.Encode(MessageTypes.ServerHello, payload);
    }

    static byte[] CorrelatedResponseFrame(ulong correlationId, ushort messageType, byte[] payload)
    {
        var frame = FrameCodec.EncodeCorrelated(correlationId, messageType, payload);
        frame[0] = (byte)MessageTypes.Correlated;
        return frame;
    }

    static ClientConfig Config(TimeSpan? timeout = null) => new(
        new Uri("ws://queued/ws"),
        AuthSettleDelay: TimeSpan.Zero,
        Retry: new RetryOptions(Enabled: false),
        Reconnect: new ReconnectOptions(Enabled: false),
        Timeout: timeout ?? TimeSpan.FromSeconds(10));

    static async Task WaitForAsync(Func<bool> condition, string because)
    {
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(5);
        }

        Assert.Fail(because);
    }

    [Fact]
    public async Task ShouldReportCorrelationEnabledGivenServerHelloWhenAdvertised()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();
        Assert.False(connection.CorrelationEnabled);

        // Act
        transport.QueueIncomingFrame(ServerHelloFrame(ServerCapabilities.CorrelationBit));

        // Assert
        await WaitForAsync(() => connection.CorrelationEnabled, "SERVER_HELLO should enable correlation.");
        Assert.Equal(1, connection.Capabilities.ProtocolVersion);
    }

    [Fact]
    public async Task ShouldNotBlockConnectGivenNoServerHelloWhenLegacyBroker()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);

        // Act: no advertisement is ever queued.
        var stopwatch = Stopwatch.StartNew();
        await connection.ConnectAsync();
        stopwatch.Stop();

        // Assert: connect completes immediately and correlation stays off, which is not an error.
        Assert.Equal(ConnectionState.Authenticated, connection.State);
        Assert.False(connection.CorrelationEnabled);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(2), $"Connect took {stopwatch.Elapsed}.");
    }

    [Fact]
    public async Task ShouldIgnoreGivenUnknownCapabilityBitsWhenAdvertised()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();

        // Act: a future capability we do not implement, alongside one we do.
        transport.QueueIncomingFrame(ServerHelloFrame(ServerCapabilities.CorrelationBit | 0x8000_0000u, protocolVersion: 99));

        // Assert: unknown bits and versions are ignored, not rejected.
        await WaitForAsync(() => connection.CorrelationEnabled, "Known bits should still apply.");
        Assert.Equal(ConnectionState.Authenticated, connection.State);
    }

    [Fact]
    public async Task ShouldLabelRequestGivenCorrelationEnabledWhenSending()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();
        transport.QueueIncomingFrame(ServerHelloFrame(ServerCapabilities.CorrelationBit));
        await WaitForAsync(() => connection.CorrelationEnabled, "Correlation should be advertised.");

        // Act
        var request = connection.RequestAsync(MessageTypes.KvGet, new byte[] { 0x1 }).AsTask();
        await WaitForAsync(() => transport.SentFrames.Count >= 2, "Request should reach the transport.");

        // Assert: the request frame carries a CORRELATE record ahead of the request itself.
        var sent = transport.SentFrames[^1];
        Assert.Equal(MessageTypes.Correlate, sent[0]);
        var correlationId = BinaryPrimitives.ReadUInt64BigEndian(sent.AsSpan(3, 8));
        Assert.NotEqual(0UL, correlationId);
        Assert.Equal(MessageTypes.KvGet, sent[FrameCodec.CorrelationRecordSize]);

        transport.QueueIncomingFrame(CorrelatedResponseFrame(correlationId, MessageTypes.KvGet, [0xAB]));
        Assert.Equal([0xAB], (await request.WaitAsync(TimeSpan.FromSeconds(10))).ToArray());
    }

    [Fact]
    public async Task ShouldSendPlainFrameGivenCorrelationUnavailableWhenSending()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();

        // Act
        var request = connection.RequestAsync(MessageTypes.KvGet, new byte[] { 0x1 }).AsTask();
        await WaitForAsync(() => transport.SentFrames.Count >= 2, "Request should reach the transport.");

        // Assert: no CORRELATE label against a broker that never advertised one.
        var sent = transport.SentFrames[^1];
        Assert.Equal(MessageTypes.KvGet, sent[0]);

        transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.KvGet, [0xCD]));
        Assert.Equal([0xCD], (await request.WaitAsync(TimeSpan.FromSeconds(10))).ToArray());
    }

    [Fact]
    public async Task ShouldResolveOwnCallerGivenOutOfOrderResponsesWhenCorrelatedEndToEnd()
    {
        // Arrange: CS-019 through the real receive loop.
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();
        transport.QueueIncomingFrame(ServerHelloFrame(ServerCapabilities.CorrelationBit));
        await WaitForAsync(() => connection.CorrelationEnabled, "Correlation should be advertised.");

        var first = connection.RequestAsync(MessageTypes.QueueReserve, new byte[] { 0x1 }).AsTask();
        var second = connection.RequestAsync(MessageTypes.QueueReserve, new byte[] { 0x2 }).AsTask();
        await WaitForAsync(() => transport.SentFrames.Count >= 3, "Both requests should reach the transport.");

        var firstId = BinaryPrimitives.ReadUInt64BigEndian(transport.SentFrames[1].AsSpan(3, 8));
        var secondId = BinaryPrimitives.ReadUInt64BigEndian(transport.SentFrames[2].AsSpan(3, 8));
        Assert.NotEqual(firstId, secondId);

        // Act: answer the second first, as a parked RESERVE forces.
        transport.QueueIncomingFrame(CorrelatedResponseFrame(secondId, MessageTypes.QueueReserve, [0xB]));
        transport.QueueIncomingFrame(CorrelatedResponseFrame(firstId, MessageTypes.QueueReserve, [0xA]));

        // Assert
        Assert.Equal([0xA], (await first.WaitAsync(TimeSpan.FromSeconds(10))).ToArray());
        Assert.Equal([0xB], (await second.WaitAsync(TimeSpan.FromSeconds(10))).ToArray());
    }

    [Fact]
    public async Task ShouldKeepSessionGivenCorrelatedRequestWhenOneRequestTimesOut()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(TimeSpan.FromMilliseconds(200)), () => transport);
        await connection.ConnectAsync();
        transport.QueueIncomingFrame(ServerHelloFrame(ServerCapabilities.CorrelationBit));
        await WaitForAsync(() => connection.CorrelationEnabled, "Correlation should be advertised.");

        // Act: abandon one correlated request, then use the same session again.
        await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            connection.RequestAsync(MessageTypes.KvGet, new byte[] { 0x1 }).AsTask());
        await Task.Delay(50);

        var next = connection.RequestAsync(MessageTypes.KvGet, new byte[] { 0x2 }).AsTask();
        await WaitForAsync(() => transport.SentFrames.Count >= 3, "The next request should use the existing transport.");
        var correlationId = BinaryPrimitives.ReadUInt64BigEndian(transport.SentFrames[^1].AsSpan(3, 8));
        transport.QueueIncomingFrame(CorrelatedResponseFrame(correlationId, MessageTypes.KvGet, [0xAB]));

        // Assert
        Assert.Equal([0xAB], (await next.WaitAsync(TimeSpan.FromSeconds(10))).ToArray());
        Assert.Equal(ConnectionState.Authenticated, connection.State);
    }

    [Fact]
    public async Task ShouldClearCorrelationGivenNewSessionWhenReconnecting()
    {
        // Arrange: capabilities belong to a session, not to the client.
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(Config(), () => transport);
        await connection.ConnectAsync();
        transport.QueueIncomingFrame(ServerHelloFrame(ServerCapabilities.CorrelationBit));
        await WaitForAsync(() => connection.CorrelationEnabled, "Correlation should be advertised.");

        // Act
        await connection.CloseAsync();
        await using var reconnected = new TestQueuedTransport();
        await using var second = new FitzConnection(Config(), () => reconnected);
        await second.ConnectAsync();

        // Assert: the new session has advertised nothing yet.
        Assert.False(second.CorrelationEnabled);
    }
}
