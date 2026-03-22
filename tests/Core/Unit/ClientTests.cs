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
        var transport = new FakeTransport();
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
        Assert.Single(transport.SentFrames);
        var frame = FrameCodec.Decode(transport.SentFrames[0]);
        Assert.Equal(MessageTypes.Connect, frame.MessageType);
        Assert.Equal("token-123", System.Text.Encoding.UTF8.GetString(frame.Payload));
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
        var transport = new FakeTransport(receive: _ => Task.FromResult(Array.Empty<byte>()));
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
    public async Task should_remain_connected_given_request_timeout_when_receive_loop_is_idle()
    {
        // Arrange
        var transport = new FakeTransport(receive: async ct =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return [];
        });
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

    private sealed class FakeTransport : ITransport
    {
        private readonly Func<CancellationToken, Task<byte[]>> _receive;

        public FakeTransport(Func<CancellationToken, Task<byte[]>>? receive = null)
        {
            _receive = receive ?? (ct => Task.Delay(50, ct).ContinueWith(_ => Array.Empty<byte>(), TaskScheduler.Default));
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

        public Task<byte[]> ReceiveAsync(CancellationToken cancellationToken = default)
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
}
