using System.Net.WebSockets;

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShouldKeepSocketOpenGivenRacingCallerCancellationWhenRpcFrameIsAlreadyWriting(bool queuedSessionEnds)
    {
        // Arrange
        using var stream = new GatedWriteStream();
        using var socket = WebSocket.CreateFromStream(stream, new WebSocketCreationOptions());
        await using var transport = new WebSocketTransport(socket, new Uri("ws://localhost/ws"), TimeSpan.FromSeconds(5), 1024);
        using var activeCaller = new CancellationTokenSource();
        using var queuedCaller = new CancellationTokenSource();
        using var queuedSession = new CancellationTokenSource();
        var attempts = 0;
        var active = transport.SendRpcRequestAsync("rpc"u8.ToArray(), () => attempts++, CancellationToken.None, activeCaller.Token);

        try
        {
            await stream.FirstWrite.Task.WaitAsync(TimeSpan.FromSeconds(2));
            var queued = transport.SendRpcRequestAsync("queued"u8.ToArray(), () => attempts++, queuedSession.Token, queuedCaller.Token);

            // Act
            await (queuedSessionEnds ? queuedSession : queuedCaller).CancelAsync();
            await activeCaller.CancelAsync();

            // Assert
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued.WaitAsync(TimeSpan.FromSeconds(2)));
            stream.ReleaseWrite.SetResult();
            await active.WaitAsync(TimeSpan.FromSeconds(2));
            await transport.SendAsync("next"u8.ToArray());
            Assert.Equal(WebSocketState.Open, socket.State);
            Assert.Equal(1, attempts);
            Assert.Equal(2, stream.Writes);
        }
        finally
        {
            stream.ReleaseWrite.TrySetResult();
            socket.Abort();
        }
    }

    [Fact]
    public async Task ShouldInterruptActiveWriteGivenSessionEndsWhenRpcFrameIsAlreadyWriting()
    {
        // Arrange
        using var stream = new GatedWriteStream();
        using var socket = WebSocket.CreateFromStream(stream, new WebSocketCreationOptions());
        await using var transport = new WebSocketTransport(socket, new Uri("ws://localhost/ws"), Timeout.InfiniteTimeSpan, 1024);
        using var session = new CancellationTokenSource();
        var active = transport.SendRpcRequestAsync("rpc"u8.ToArray(), () => { }, session.Token, CancellationToken.None);

        try
        {
            await stream.FirstWrite.Task.WaitAsync(TimeSpan.FromSeconds(2));

            // Act
            await session.CancelAsync();

            // Assert
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => active.WaitAsync(TimeSpan.FromSeconds(2)));
            Assert.Equal(WebSocketState.Aborted, socket.State);
        }
        finally
        {
            stream.ReleaseWrite.TrySetResult();
            socket.Abort();
        }
    }

    [Fact]
    public async Task ShouldAbortPartialFrameGivenStalledWriteWhenTransportClockReachesDeadline()
    {
        // Arrange
        using var stream = new GatedWriteStream();
        using var socket = WebSocket.CreateFromStream(stream, new WebSocketCreationOptions());
        var time = new ManualTimeProvider();
        var timeout = TimeSpan.FromMinutes(1);
        await using var transport = new WebSocketTransport(socket, new Uri("ws://localhost/ws"), timeout, 1024, time);

        try
        {
            // Act
            var sending = transport.SendAsync("frame"u8.ToArray());
            await stream.FirstWrite.Task.WaitAsync(TimeSpan.FromSeconds(5));
            time.Advance(timeout - TimeSpan.FromTicks(1));
            var completedEarly = sending.IsCompleted;
            time.Advance(TimeSpan.FromTicks(1));
            var error = await Record.ExceptionAsync(() => sending.WaitAsync(TimeSpan.FromSeconds(5)));

            // Assert
            Assert.False(completedEarly);
            Assert.Contains("WebSocket send exceeded", Assert.IsType<TimeoutException>(error).Message,
                StringComparison.Ordinal);
            Assert.Equal(WebSocketState.Aborted, socket.State);
        }
        finally
        {
            stream.ReleaseWrite.TrySetResult();
            socket.Abort();
        }
    }

    sealed class GatedWriteStream : MemoryStream
    {
        internal TaskCompletionSource FirstWrite { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource ReleaseWrite { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Writes { get; private set; }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Write(buffer.Span);
            Writes++;

            if (Writes == 1)
            {
                FirstWrite.SetResult();
                await ReleaseWrite.Task.WaitAsync(cancellationToken);
            }
        }
    }
}
