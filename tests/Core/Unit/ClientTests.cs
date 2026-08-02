using System.Buffers;
using System.Threading.Channels;
using Cntryl.Fitz;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ClientTests
{
    [Fact]
    public async Task should_close_once_given_repeated_close_calls()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var client = new Client(new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            TransportFactory: _ => transport));

        // Act
        await client.CloseAsync();
        await client.CloseAsync();

        // Assert
        Assert.Equal(ConnectionState.Closed, client.State);
    }

    [Fact]
    public void should_expose_typed_transport_given_typed_client_config()
    {
        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"), ClientTransport.WebSocket);

        Assert.Equal(ClientTransport.WebSocket, config.Transport);
    }

    [Fact]
    public void should_preserve_explicit_transport_given_client_config()
    {
        var config = new ClientConfig(new Uri("tcp://localhost:4191"), Transport: ClientTransport.Tcp);

        Assert.Equal(ClientTransport.Tcp, config.Transport);
    }

    [Fact]
    public void should_default_max_in_flight_requests_when_not_specified()
    {
        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"));

        Assert.Equal(256, config.MaxInFlightRequests);
    }

    [Fact]
    public void should_preserve_max_in_flight_requests_given_client_config()
    {
        var config = new ClientConfig(new Uri("ws://localhost:4190/ws"), MaxInFlightRequests: 12);

        Assert.Equal(12, config.MaxInFlightRequests);
    }

    [Fact]
    public void should_resolve_transport_given_websocket_and_tcp_endpoints_when_configured()
    {
        var websocket = new ClientConfig(new Uri("ws://localhost:4190/ws"));
        var tcp = new ClientConfig(new Uri("tcp://localhost:4191"));

        Assert.Equal(ClientTransport.Auto, websocket.Transport);
        Assert.Equal(ClientTransport.WebSocket, websocket.ResolvedTransportKind);
        Assert.Equal(ClientTransport.Auto, tcp.Transport);
        Assert.Equal(ClientTransport.Tcp, tcp.ResolvedTransportKind);
        Assert.Equal(1024, websocket.MaxRequestQueueSize);
        Assert.True(websocket.ResolvedReconnect.Enabled);
        Assert.True(websocket.ResolvedRetry.Enabled);
        Assert.True(websocket.ResolvedHeartbeat.Enabled);
    }

    [Fact]
    public async Task should_authenticate_given_valid_jwt_when_connect_frame_is_sent_first()
    {
        // Arrange
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        var config = new ClientConfig(
            new Uri("ws://localhost:4190/ws"),
            AuthSettleDelay: TimeSpan.Zero,
            TransportFactory: _ => transport,
            TokenProvider: _ => ValueTask.FromResult("token-123")
        );
        await using var client = new Client(config);

        // Act
        await client.ConnectAsync();

        // Assert
        Assert.Equal(ConnectionState.Authenticated, client.State);
        Assert.True(client.IsConnected);
        Assert.Single(transport.SentFrames);
        var frame = FrameCodec.Decode(transport.SentFrames[0]);
        Assert.Equal(MessageTypes.Connect, frame.MessageType);
        Assert.Equal("token-123", System.Text.Encoding.UTF8.GetString(frame.Payload.Span));
    }

    [Fact]
    public async Task should_throw_operation_canceled_given_canceled_token_when_connecting()
    {
        // Arrange
        await using var transport = new FakeTransport();
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.FromSeconds(5),
                TransportFactory: _ => transport
            )
        );

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var act = () => client.ConnectAsync(cts.Token);

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
    }

    [Fact]
    public async Task should_throw_authentication_exception_given_transport_close_during_authentication()
    {
        // Arrange
        await using var transport = new FakeTransport(receive: _ => new ValueTask<PooledFrame>(PooledFrame.Closed));
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.FromMilliseconds(200),
                TransportFactory: _ => transport
            )
        );

        // Act
        var act = () => client.ConnectAsync();

        // Assert
        await Assert.ThrowsAsync<AuthenticationException>(act);
    }

    [Fact]
    public async Task should_retry_startup_transport_failures_given_connect_when_ready()
    {
        var attempts = 0;
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromMilliseconds(100),
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ =>
                {
                    attempts++;
                    return attempts < 2
                        ? new FailingConnectTransport(new IOException("dial failed"))
                        : new IdleTransport();
                }));

        await client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
            Timeout: TimeSpan.FromMilliseconds(1000),
            Backoff: TimeSpan.FromMilliseconds(1),
            MaxBackoff: TimeSpan.FromMilliseconds(1)));

        Assert.Equal(2, attempts);
        Assert.True(client.IsConnected);
    }

    [Fact]
    public async Task should_not_retry_authentication_given_rejected_connect_when_reconnect_enabled()
    {
        var attempts = 0;
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.FromMilliseconds(200),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new FakeTransport(_ => new ValueTask<PooledFrame>(PooledFrame.Closed));
                }));

        await Assert.ThrowsAsync<AuthenticationException>(() =>
            client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
                Timeout: TimeSpan.FromMilliseconds(250),
                Backoff: TimeSpan.FromMilliseconds(1),
                MaxBackoff: TimeSpan.FromMilliseconds(1))));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task should_timeout_given_connect_when_ready_total_deadline_expires()
    {
        var attempts = 0;
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new FailingConnectTransport(new IOException("dial failed"));
                }));

        await Assert.ThrowsAsync<TimeoutException>(() =>
            client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
                Timeout: TimeSpan.FromMilliseconds(50),
                Backoff: TimeSpan.FromMilliseconds(1),
                MaxBackoff: TimeSpan.FromMilliseconds(1))));

        Assert.True(attempts >= 1);
    }

    [Fact]
    public async Task should_coalesce_concurrent_connect_calls_onto_one_inflight_attempt()
    {
        var releaseConnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                TransportFactory: _ =>
                {
                    attempts++;
                    return new BlockingConnectTransport(releaseConnect.Task);
                }));

        var first = client.ConnectAsync();
        var second = client.ConnectAsync();
        await Task.Delay(25);

        Assert.Equal(1, attempts);

        releaseConnect.TrySetResult();
        await Task.WhenAll(first, second);
        Assert.True(client.IsConnected);
    }

    [Fact]
    public async Task should_stop_reconnecting_given_close_during_backoff_when_close_called()
    {
        var releaseFirstReceive = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var firstTransport = new QueuedTransport();
        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        _ = Task.Run(async () =>
        {
            await releaseFirstReceive.Task;
            firstTransport.QueueClosed();
        });
        await using var secondTransport = new QueuedTransport();
        var factoryCalls = 0;

        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(250), MaxBackoff: TimeSpan.FromMilliseconds(250)),
                TransportFactory: _ => factoryCalls++ == 0 ? firstTransport : secondTransport
            )
        );

        await client.ConnectAsync();

        releaseFirstReceive.SetResult();
        await WaitForConditionAsync(() => !client.IsConnected, TimeSpan.FromSeconds(1));

        await client.DisposeAsync();

        Assert.Equal(1, factoryCalls);
        Assert.Empty(secondTransport.SentFrames);
        Assert.False(client.IsConnected);
    }

    [Fact]
    public async Task should_remain_connected_given_request_timeout_when_receive_loop_is_idle()
    {
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Timeout: TimeSpan.FromMilliseconds(50),
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ => transport
            )
        );

        await client.ConnectAsync();

        var ex = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            client.Kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async));

        Assert.Contains("Request timeout", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(client.IsConnected);
    }

    [Fact]
    public async Task should_bound_concurrent_outbound_requests_given_max_one_when_second_request_starts()
    {
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                MaxInFlightRequests: 1,
                TokenProvider: _ => ValueTask.FromResult("token-123")
            ),
            () => transport
        );

        await connection.ConnectAsync();

        var firstRequest = connection.RequestAsync(77, "first"u8.ToArray());
        await WaitForConditionAsync(() => transport.SentFrames.Count == 2, TimeSpan.FromSeconds(1));

        var secondRequest = connection.RequestAsync(77, "second"u8.ToArray());

        await Task.Delay(50);
        Assert.False(secondRequest.IsCompleted);
        Assert.Equal(2, transport.SentFrames.Count);

        transport.QueueIncomingFrame(FrameCodec.Encode(77, "ok"u8));
        Assert.Equal("ok", System.Text.Encoding.UTF8.GetString((await firstRequest).Span));

        await WaitForConditionAsync(() => transport.SentFrames.Count == 3, TimeSpan.FromSeconds(1));
        transport.QueueIncomingFrame(FrameCodec.Encode(77, "done"u8));
        Assert.Equal("done", System.Text.Encoding.UTF8.GetString((await secondRequest).Span));

        await connection.CloseAsync();
    }

    [Fact]
    public async Task should_throw_request_queue_full_given_waiter_limit_reached()
    {
        await using var transport = new QueuedTransport();
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                MaxInFlightRequests: 1,
                MaxRequestQueueSize: 1),
            () => transport);
        try
        {
            await connection.ConnectAsync();

            var first = connection.RequestAsync(88, "first"u8.ToArray());
            await WaitForConditionAsync(() => transport.SentFrames.Count == 2, TimeSpan.FromSeconds(1));

            var second = connection.RequestAsync(88, "second"u8.ToArray());
            await Task.Delay(25);
            Assert.False(second.IsCompleted);

            var ex = await Assert.ThrowsAsync<RequestQueueFullException>(() =>
                connection.RequestAsync(88, "third"u8.ToArray()).AsTask());

            Assert.Contains("queue", ex.Message, StringComparison.OrdinalIgnoreCase);

            transport.QueueIncomingFrame(FrameCodec.Encode(88, "ok"u8));
            await WaitForConditionAsync(() => transport.SentFrames.Count == 3, TimeSpan.FromSeconds(1));
            transport.QueueIncomingFrame(FrameCodec.Encode(88, "done"u8));
            await Task.WhenAll(first.AsTask(), second.AsTask());
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    [Fact]
    public async Task should_receive_notice_notification_given_connection_backed_subscription()
    {
        await using var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        await using var client = new Client(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ => transport));

        await client.ConnectAsync();

        var subscribeTask = client.Notice.SubscribeAsync("notice://prod/app/*");

        using (var subscribeResponse = new BinaryBufferWriter())
        {
            subscribeResponse.WriteU8(0);
            subscribeResponse.WriteU8(1);
            subscribeResponse.WriteU64(55);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.NoticeSubscribe, subscribeResponse.WrittenSpan));
        }

        var subscription = await subscribeTask;
        var received = ReadFirstAsync(subscription);
        const ulong subscriptionId = 55;

        using (var notification = new BinaryBufferWriter())
        {
            notification.WriteU64(subscriptionId);
            notification.WriteString("notice://prod/app/events");
            notification.WriteU32(5);
            notification.WriteBytes("hello"u8);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.NoticeNotify, notification.WrittenSpan));
        }

        var message = await received.WaitAsync(TimeSpan.FromSeconds(1));
        var result = (message.Route, Body: message.Body.ToArray());

        Assert.Equal("notice://prod/app/events", result.Route);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(result.Body));

        var disposeTask = subscription.DisposeAsync().AsTask();
        using (var unsubscribeResponse = new BinaryBufferWriter())
        {
            unsubscribeResponse.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(
                MessageTypes.NoticeUnsubscribe,
                unsubscribeResponse.WrittenSpan));
        }
        await disposeTask;
    }

    private static async Task<T> ReadFirstAsync<T>(IAsyncEnumerable<T> notifications)
    {
        await foreach (var notification in notifications)
        {
            return notification;
        }

        throw new InvalidOperationException("Subscription completed before a notification arrived");
    }

    private static async Task WaitForConditionAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;

        while (!condition())
        {
            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for the requested condition.");
            }

            await Task.Delay(10);
        }
    }

    private sealed class FakeTransport : ITransport
    {
        private readonly Func<CancellationToken, ValueTask<PooledFrame>> _receive;

        public FakeTransport(Func<CancellationToken, ValueTask<PooledFrame>>? receive = null)
        {
            _receive = receive ?? (_ => new ValueTask<PooledFrame>(PooledFrame.Closed));
        }

        public List<byte[]> SentFrames { get; } = [];

        public Uri Url { get; } = new("ws://fake");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SentFrames.Add(data.ToArray());
            return Task.CompletedTask;
        }

        public ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _receive(cancellationToken);
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FailingConnectTransport : ITransport
    {
        private readonly Exception _exception;

        public FailingConnectTransport(Exception exception)
        {
            _exception = exception;
        }

        public Uri Url { get; } = new("ws://failing");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromException(_exception);
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<PooledFrame>(PooledFrame.Closed);
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class IdleTransport : ITransport
    {
        public Uri Url { get; } = new("ws://idle");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            return PooledFrame.Closed;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class BlockingConnectTransport : ITransport
    {
        private readonly Task _connectSignal;

        public BlockingConnectTransport(Task connectSignal)
        {
            _connectSignal = connectSignal;
        }

        public Uri Url { get; } = new("ws://blocking");

        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _connectSignal.WaitAsync(cancellationToken);
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken);
            return PooledFrame.Closed;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class QueuedTransport : ITransport
    {
        private readonly Channel<PooledFrame> _incoming = Channel.CreateUnbounded<PooledFrame>();
        private readonly object _sentFramesGate = new();

        public List<byte[]> SentFrames { get; } = [];
        public Action<int>? AfterSend { get; set; }

        public Uri Url { get; } = new("ws://queued");

        public Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int sentFrameCount;
            lock (_sentFramesGate)
            {
                SentFrames.Add(data.ToArray());
                sentFrameCount = SentFrames.Count;
            }

            AfterSend?.Invoke(sentFrameCount);

            return Task.CompletedTask;
        }

        public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await _incoming.Reader.ReadAsync(cancellationToken);
        }

        public void QueueIncomingFrame(byte[] frame)
        {
            ArgumentNullException.ThrowIfNull(frame);

            var buffer = ArrayPool<byte>.Shared.Rent(frame.Length);
            frame.AsSpan().CopyTo(buffer);
            _incoming.Writer.TryWrite(PooledFrame.FromRentedBuffer(buffer, frame.Length));
        }

        public void QueueClosed()
        {
            _incoming.Writer.TryWrite(PooledFrame.Closed);
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _incoming.Writer.TryComplete();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            _incoming.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }
    }
}
