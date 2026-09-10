using System.Net.WebSockets;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class WebSocketTransportTests
{
    [Fact]
    public void ShouldConfigureNativeWebsocketPingTimeout()
    {
        using var socket = new ClientWebSocket();
        var heartbeat = new HeartbeatOptions(
            Interval: TimeSpan.FromSeconds(7),
            Timeout: TimeSpan.FromSeconds(3));

        WebSocketTransport.ConfigureHeartbeat(socket.Options, heartbeat);

        Assert.Equal(TimeSpan.FromSeconds(7), socket.Options.KeepAliveInterval);
        Assert.Equal(TimeSpan.FromSeconds(3), socket.Options.KeepAliveTimeout);
    }

    [Fact]
    public async Task ShouldRejectSendLargerThanConfiguredFrameCap()
    {
        await using var transport = new WebSocketTransport(
            new Uri("ws://localhost:4190/ws"),
            TimeSpan.FromSeconds(1),
            maxFrameSize: 8);

        var error = await Assert.ThrowsAsync<ProtocolException>(() => transport.SendAsync(new byte[9]));
        Assert.Contains("8", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ShouldRejectTextMessageGivenWebsocketReceiveFrame()
    {
        var error = Assert.Throws<ProtocolException>(() =>
            WebSocketTransport.EnsureBinaryMessage(WebSocketMessageType.Text));

        Assert.Contains("text frames", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ShouldAcceptBinaryMessageGivenWebsocketReceiveFrame() => WebSocketTransport.EnsureBinaryMessage(WebSocketMessageType.Binary);
}
