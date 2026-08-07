using System.Net.WebSockets;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class WebSocketTransportTests
{
    [Fact]
    public void should_reject_text_message_given_websocket_receive_frame()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            WebSocketTransport.EnsureBinaryMessage(WebSocketMessageType.Text));

        Assert.Contains("text frames", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void should_accept_binary_message_given_websocket_receive_frame()
    {
        WebSocketTransport.EnsureBinaryMessage(WebSocketMessageType.Binary);
    }
}
