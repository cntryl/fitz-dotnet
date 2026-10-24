using System.Buffers.Binary;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ConnectionNegotiationTests
{
    internal static byte[] HelloFrame(uint capabilities = 0, ulong sessionId = 1)
    {
        var payload = new byte[22];
        BinaryPrimitives.WriteUInt16BigEndian(payload, 2);
        BinaryPrimitives.WriteUInt32BigEndian(payload.AsSpan(2), capabilities);
        BinaryPrimitives.WriteUInt32BigEndian(payload.AsSpan(6), 1024 * 1024);
        BinaryPrimitives.WriteUInt32BigEndian(payload.AsSpan(10), 64);
        BinaryPrimitives.WriteUInt64BigEndian(payload.AsSpan(14), sessionId);
        return FrameCodec.Encode(MessageTypes.Hello, payload);
    }

    [Fact]
    public async Task ShouldWaitForHelloGivenNoOptionalCapabilitiesWhenConnecting()
    {
        // Arrange
        await using var transport = new TestQueuedTransport { AutoHello = false };
        var config = new ClientConfig(new Uri("ws://queued/ws"), AuthSettleDelay: TimeSpan.Zero,
            Reconnect: new ReconnectOptions(Enabled: false));
        await using var connection = new FitzConnection(config, () => transport);

        // Act
        var connecting = connection.ConnectAsync();
        await Task.Delay(25);
        var completedBeforeHello = connecting.IsCompleted;
        transport.QueueIncomingFrame(HelloFrame());
        await connecting.WaitAsync(TimeSpan.FromSeconds(2));

        // Assert
        Assert.False(completedBeforeHello);
        Assert.True(connection.CorrelationEnabled);
        Assert.Equal(2, connection.Capabilities.ProtocolVersion);
        Assert.Equal(64u, connection.Capabilities.MaxInFlightRequests);
    }
}
