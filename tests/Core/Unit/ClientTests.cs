using System.Buffers;
using System.Threading.Channels;
using Cntryl.Fitz;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ClientTests
{
    [Fact]
    public async Task should_set_connected_state_given_valid_transport_when_connecting()
    {
        // Arrange
        var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 2)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        var config = new ClientConfig(
            "ws://localhost:4190/ws",
            AuthSettleDelay: TimeSpan.Zero,
            TransportFactory: _ => transport,
            TokenProvider: _ => ValueTask.FromResult("token-123")
        );
        var client = new Client(config);

        // Act
        await client.ConnectAsync();

        // Assert
        Assert.True(client.IsConnected);
        Assert.Equal(2, transport.SentFrames.Count);
        var frame = FrameCodec.Decode(transport.SentFrames[0]);
        Assert.Equal(MessageTypes.Connect, frame.MessageType);
        Assert.Equal("token-123", System.Text.Encoding.UTF8.GetString(frame.Payload.Span));

        var probeFrame = FrameCodec.Decode(transport.SentFrames[1]);
        Assert.Equal(MessageTypes.LeaseQuery, probeFrame.MessageType);
        var probeReader = new BinaryBufferReader(probeFrame.Payload.ToArray());
        Assert.Equal("lease://fitz/system/auth-probe", probeReader.ReadString());
    }

    [Fact]
    public async Task should_throw_operation_canceled_given_canceled_token_when_connecting()
    {
        // Arrange
        var transport = new FakeTransport();
        var client = new Client(
            new ClientConfig(
                "ws://localhost:4190/ws",
                AuthSettleDelay: TimeSpan.FromSeconds(5),
                TransportFactory: _ => transport
            )
        );

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act
        var act = () => client.ConnectAsync(cts.Token);

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
    }

    [Fact]
    public async Task should_throw_authentication_exception_given_transport_close_during_authentication()
    {
        // Arrange
        var transport = new FakeTransport(receive: _ => new ValueTask<PooledFrame>(PooledFrame.Closed));
        var client = new Client(
            new ClientConfig(
                "ws://localhost:4190/ws",
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
    public async Task should_not_reconnect_given_close_during_reconnect_backoff()
    {
        // Arrange
        var releaseFirstReceive = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var authResponseDelivered = false;
        var firstTransport = new FakeTransport(async _ =>
        {
            if (!authResponseDelivered)
            {
                authResponseDelivered = true;
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                var frame = FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan);
                var buffer = ArrayPool<byte>.Shared.Rent(frame.Length);
                frame.CopyTo(buffer);
                return PooledFrame.FromRentedBuffer(buffer, frame.Length);
            }

            await releaseFirstReceive.Task.ConfigureAwait(false);
            return PooledFrame.Closed;
        });
        var secondTransport = new FakeTransport();
        var factoryCalls = 0;

        var client = new Client(
            new ClientConfig(
                "ws://localhost:4190/ws",
                AuthSettleDelay: TimeSpan.Zero,
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(250), MaxBackoff: TimeSpan.FromMilliseconds(250)),
                TransportFactory: _ => factoryCalls++ == 0 ? firstTransport : secondTransport
            )
        );

        await client.ConnectAsync();

        releaseFirstReceive.SetResult();
        await WaitForConditionAsync(() => !client.IsConnected, TimeSpan.FromSeconds(1));

        // Act
        await client.DisposeAsync();

        // Assert
        Assert.Equal(1, factoryCalls);
        Assert.Empty(secondTransport.SentFrames);
        Assert.False(client.IsConnected);

        static async Task WaitForConditionAsync(Func<bool> condition, TimeSpan timeout)
        {
            var deadline = DateTimeOffset.UtcNow + timeout;

            while (!condition())
            {
                if (DateTimeOffset.UtcNow >= deadline)
                {
                    throw new TimeoutException("Timed out waiting for the connection to disconnect.");
                }

                await Task.Delay(10).ConfigureAwait(false);
            }
        }
    }

    [Fact]
    public async Task should_remain_connected_given_request_timeout_when_receive_loop_is_idle()
    {
        // Arrange
        var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 2)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        var client = new Client(
            new ClientConfig(
                "ws://localhost:4190/ws",
                Timeout: TimeSpan.FromMilliseconds(50),
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ => transport
            )
        );

        await client.ConnectAsync();

        // Act
        var ex = await Assert.ThrowsAsync<RequestTimeoutException>(() => client.RequestAsync(MessageTypes.KvBegin, []));

        // Assert
        Assert.Contains("Request timeout", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(client.IsConnected);
    }

    [Fact]
    public async Task should_receive_notice_notification_given_connection_backed_subscription()
    {
        var transport = new QueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 2)
            {
                return;
            }

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan));
        };
        await using var client = new Client(
            new ClientConfig(
                "ws://localhost:4190/ws",
                AuthSettleDelay: TimeSpan.Zero,
                TransportFactory: _ => transport));

        await client.ConnectAsync();

        var receivedTcs = new TaskCompletionSource<(string Route, byte[] Body)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscribeTask = client.Notice().SubscribeAsync("notice://prod/app/*", (message, _) =>
        {
            receivedTcs.TrySetResult((message.Route, message.Body.ToArray()));
            return ValueTask.CompletedTask;
        });

        using (var subscribeResponse = new BinaryBufferWriter())
        {
            subscribeResponse.WriteU8(0);
            subscribeResponse.WriteU8(1);
            subscribeResponse.WriteU64(55);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.NoticeSubscribe, subscribeResponse.WrittenSpan));
        }

        var subscription = await subscribeTask;

        using (var notification = new BinaryBufferWriter())
        {
            notification.WriteU64(subscription.SubscriptionId);
            notification.WriteString("notice://prod/app/events");
            notification.WriteU32(5);
            notification.WriteBytes("hello"u8);
            transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.NoticeNotify, notification.WrittenSpan));
        }

        var result = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.Equal("notice://prod/app/events", result.Route);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(result.Body));
    }

    private sealed class FakeTransport : ITransport
    {
        private readonly Func<CancellationToken, ValueTask<PooledFrame>> _receive;

        public FakeTransport(Func<CancellationToken, ValueTask<PooledFrame>>? receive = null)
        {
            _receive = receive ?? (_ => new ValueTask<PooledFrame>(PooledFrame.Closed));
        }

        public List<byte[]> SentFrames { get; } = [];

        public string Url => "ws://fake";

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

    private sealed class QueuedTransport : ITransport
    {
        private readonly Channel<byte[]> _incoming = Channel.CreateUnbounded<byte[]>();
        private readonly object _sentFramesGate = new();

        public List<byte[]> SentFrames { get; } = [];
        public Action<int>? AfterSend { get; set; }

        public string Url => "ws://queued";

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

            var frame = await _incoming.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            var buffer = ArrayPool<byte>.Shared.Rent(frame.Length);
            frame.AsSpan().CopyTo(buffer);
            return PooledFrame.FromRentedBuffer(buffer, frame.Length);
        }

        public void QueueIncomingFrame(byte[] frame)
        {
            ArgumentNullException.ThrowIfNull(frame);
            _incoming.Writer.TryWrite(frame);
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
