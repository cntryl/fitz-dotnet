using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using Cntryl.Fitz.Protocol;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class WorkloadIdentityTests
{
    [Fact]
    public async Task ShouldSendFreshTokenGivenRotatingProviderWhenTcpReconnects()
    {
        // Arrange
        using var deadline = StartDeadline();
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var broker = new TokenBroker();
        var server = broker.ServeTcpAsync(listener, deadline.Token);
        var endpoint = new Uri($"tcp://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}");

        // Act
        string[] tokens;

        try
        {
            tokens = await ConnectTwiceAsync(endpoint, broker, deadline.Token);
        }
        finally
        {
            await deadline.CancelAsync();

            try
            {
                await server;
            }
            finally
            {
                listener.Dispose();
            }
        }

        // Assert
        Assert.Equal(["initial-token", "rotated-token"], tokens);
    }

    [Fact]
    public async Task ShouldSendFreshTokenGivenRotatingProviderWhenWebSocketReconnects()
    {
        // Arrange
        using var deadline = StartDeadline();
        var broker = new TokenBroker();
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0));
        await using var app = builder.Build();
        app.UseWebSockets();
        app.Run(context => broker.ServeWebSocketAsync(context, deadline.Token));
        await app.StartAsync(deadline.Token);
        var endpoint = new UriBuilder(app.Urls.Single()) { Scheme = "ws", Path = "/ws" }.Uri;

        // Act
        string[] tokens;

        try
        {
            tokens = await ConnectTwiceAsync(endpoint, broker, deadline.Token);
        }
        finally
        {
            await deadline.CancelAsync();
        }

        // Assert
        Assert.Equal(["initial-token", "rotated-token"], tokens);
    }

    [Fact]
    public async Task ShouldSurfaceAcquisitionFailureGivenTokenProviderFailureWhenConnecting()
    {
        // Arrange
        using var deadline = StartDeadline();
        await using var transport = new TestQueuedTransport();
        var failure = new InvalidOperationException("Token acquisition failed.");
        await using var client = new FitzClient(new ClientConfig(transport.Url)
        {
            TransportFactory = _ => transport,
            TokenProvider = _ => ValueTask.FromException<string>(failure),
        });

        // Act
        var error = await Record.ExceptionAsync(() => client.ConnectAsync(deadline.Token));

        // Assert
        Assert.Same(failure, error);
        Assert.Empty(transport.SentFrames);
    }

    [Fact]
    public async Task ShouldCancelAcquisitionGivenWaitingTokenProviderWhenClientCloses()
    {
        // Arrange
        using var deadline = StartDeadline();
        await using var transport = new TestQueuedTransport();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new FitzClient(new ClientConfig(transport.Url)
        {
            TransportFactory = _ => transport,
            TokenProvider = async ct =>
            {
                started.TrySetResult();

                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return "unreachable-token";
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    canceled.TrySetResult();
                    throw;
                }
            },
        });
        var connect = client.ConnectAsync(deadline.Token);
        await started.Task.WaitAsync(deadline.Token);

        // Act
        await client.CloseAsync(deadline.Token);

        // Assert
        await canceled.Task.WaitAsync(deadline.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => connect);
        Assert.Empty(transport.SentFrames);
    }

    [Fact]
    public async Task ShouldOmitConnectGivenFailedRefreshWhenTransportReconnects()
    {
        // Arrange
        using var deadline = StartDeadline();
        await using var initial = new TestQueuedTransport();
        await using var failed = new TestQueuedTransport();
        await using var retry = new TestQueuedTransport();
        var factoryCalls = 0;
        var calls = 0;
        var restored = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        retry.AfterSend = _ => restored.TrySetResult();
        await using var client = new FitzClient(new ClientConfig(initial.Url)
        {
            Reconnect = new ReconnectOptions(true, MaxAttempts: 2, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            TransportFactory = _ => Interlocked.Increment(ref factoryCalls) switch
            {
                1 => initial,
                2 => failed,
                _ => retry,
            },
            TokenProvider = _ => Interlocked.Increment(ref calls) switch
            {
                1 => ValueTask.FromResult("initial-token"),
                2 => ValueTask.FromException<string>(new InvalidOperationException("Refresh failed.")),
                _ => ValueTask.FromResult("rotated-token"),
            },
        });
        await client.ConnectAsync(deadline.Token);

        // Act
        initial.QueueClosed();
        await restored.Task.WaitAsync(deadline.Token);

        // Assert
        Assert.Empty(failed.SentFrames);
    }

    static CancellationTokenSource StartDeadline()
    {
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        return deadline;
    }

    static async Task<string[]> ConnectTwiceAsync(Uri endpoint, TokenBroker broker, CancellationToken ct)
    {
        var calls = 0;
        await using var client = new FitzClient(new ClientConfig(endpoint)
        {
            Reconnect = new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            Heartbeat = new HeartbeatOptions(Enabled: false),
            TokenProvider = token =>
            {
                token.ThrowIfCancellationRequested();
                return ValueTask.FromResult(Interlocked.Increment(ref calls) == 1 ? "initial-token" : "rotated-token");
            },
        });
        await client.ConnectAsync(ct);
        broker.DropFirst.TrySetResult();
        var first = await broker.FirstToken.Task.WaitAsync(ct);
        var second = await broker.SecondToken.Task.WaitAsync(ct);
        await client.ConnectAsync(ct);
        Assert.Equal(2, Volatile.Read(ref calls));
        return [first, second];
    }

    sealed class TokenBroker
    {
        int _connections;
        public TaskCompletionSource<string> FirstToken { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<string> SecondToken { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource DropFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task ServeTcpAsync(TcpListener listener, CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    using var client = await listener.AcceptTcpClientAsync(ct);
                    await using var stream = client.GetStream();
                    var header = new byte[4];
                    await stream.ReadExactlyAsync(header, ct);
                    var length = BinaryPrimitives.ReadUInt32BigEndian(header);

                    if (length is 0 or > 4096)
                    {
                        throw new InvalidDataException("Unexpected CONNECT frame length.");
                    }

                    var payload = new byte[length];
                    await stream.ReadExactlyAsync(payload, ct);
                    var hello = ConnectionNegotiationTests.HelloFrame();
                    BinaryPrimitives.WriteUInt32BigEndian(header, (uint)hello.Length);
                    await stream.WriteAsync(header, ct);
                    await stream.WriteAsync(hello, ct);

                    if (CaptureConnect(payload))
                    {
                        await DropFirst.Task.WaitAsync(ct);
                    }
                    else
                    {
                        _ = await stream.ReadAsync(header, ct);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
        }

        public async Task ServeWebSocketAsync(HttpContext context, CancellationToken ct)
        {
            using var socket = await context.WebSockets.AcceptWebSocketAsync();
            var buffer = new byte[4096];
            var length = 0;
            ValueWebSocketReceiveResult received;
            do
            {
                received = await socket.ReceiveAsync(buffer.AsMemory(length), ct);

                if (received.MessageType != WebSocketMessageType.Binary || received.Count == 0)
                {
                    throw new InvalidDataException("Expected a binary CONNECT frame.");
                }

                length += received.Count;
            } while (!received.EndOfMessage);

            await socket.SendAsync(ConnectionNegotiationTests.HelloFrame().AsMemory(),
                WebSocketMessageType.Binary, endOfMessage: true, ct);

            try
            {
                if (CaptureConnect(buffer.AsMemory(0, length)))
                {
                    await DropFirst.Task.WaitAsync(ct);
                    context.Abort();
                }
                else
                {
                    var close = await socket.ReceiveAsync(buffer.AsMemory(), ct);
                    Assert.Equal(WebSocketMessageType.Close, close.MessageType);
                    await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
        }

        bool CaptureConnect(ReadOnlyMemory<byte> payload)
        {
            var frame = FrameCodec.DecodeStrict(payload);
            Assert.Equal(MessageTypes.Connect, frame.MessageType);
            var connect = new BinaryBufferReader(ExtensionCodec.Decode(frame).Core);
            _ = connect.ReadU16();
            _ = connect.ReadU16();
            _ = connect.ReadU32();
            var token = connect.ReadString();

            if (Interlocked.Increment(ref _connections) == 1)
            {
                FirstToken.TrySetResult(token);
                return true;
            }

            SecondToken.TrySetResult(token);
            return false;
        }
    }
}
