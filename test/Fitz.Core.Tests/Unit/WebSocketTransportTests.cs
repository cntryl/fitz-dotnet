using System.Net.WebSockets;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class WebSocketTransportTests
{
    [Theory]
    [InlineData("http://localhost:4190/ws", "ws://localhost:4190/ws")]
    [InlineData("https://localhost/ws", "wss://localhost/ws")]
    public void ShouldNormalizeHttpSchemeGivenAutomaticWebSocketTransportWhenTransportOperationRuns(string configured, string expected)
    {
        // Arrange
        // Act
        // Assert
        var transport = Assert.IsType<WebSocketTransport>(TransportResolver.Resolve(new ClientConfig(new Uri(configured))));

        Assert.Equal(new Uri(expected), transport.Url);
    }

    [Fact]
    public void ShouldConfigureNativeWebsocketPingTimeoutGivenWebSocketTransportWhenOperationRuns()
    {
        // Arrange
        using var socket = new ClientWebSocket();
        var heartbeat = new HeartbeatOptions(
            Interval: TimeSpan.FromSeconds(7),
            Timeout: TimeSpan.FromSeconds(3));


        // Act
        WebSocketTransport.ConfigureHeartbeat(socket.Options, heartbeat);


        // Assert
        Assert.Equal(TimeSpan.FromSeconds(7), socket.Options.KeepAliveInterval);
        Assert.Equal(TimeSpan.FromSeconds(3), socket.Options.KeepAliveTimeout);
    }

    [Fact]
    public async Task ShouldRejectSendLargerThanConfiguredFrameCapGivenWebSocketTransportWhenOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        await using var transport = new WebSocketTransport(
            new Uri("ws://localhost:4190/ws"),
            TimeSpan.FromSeconds(1),
            maxFrameSize: 8);

        var error = await Assert.ThrowsAsync<ProtocolException>(() => transport.SendAsync(new byte[9]));
        Assert.Contains("8", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ShouldRejectTextMessageGivenWebsocketReceiveFrameWhenTransportOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        var error = Assert.Throws<ProtocolException>(() =>
            WebSocketTransport.EnsureBinaryMessage(WebSocketMessageType.Text));

        Assert.Contains("text frames", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ShouldAcceptBinaryMessageGivenWebsocketReceiveFrameWhenTransportOperationRuns() => WebSocketTransport.EnsureBinaryMessage(WebSocketMessageType.Binary);
}
