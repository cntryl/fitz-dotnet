using System.Buffers.Binary;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Observability;
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

    [Fact]
    public async Task ShouldSurfaceVersionMismatchGivenRejectedConnectWhenWaitingForReady()
    {
        // Arrange
        var attempts = 0;
        await using var transport = new TestQueuedTransport { AutoHello = false };
        using var payload = new BinaryBufferWriter();
        payload.WriteU16(50);
        payload.WriteString("no common wire version");
        transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.ConnectRejected, payload.Build()));
        await using var client = new Client(new ClientConfig(new Uri("ws://queued/ws"), TransportFactory: _ =>
        {
            attempts++;
            return transport;
        }));

        // Act
        var error = await Record.ExceptionAsync(() => client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
            Timeout: TimeSpan.FromSeconds(1), Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))));

        // Assert
        var mismatch = Assert.IsType<ProtocolException>(error);
        Assert.Equal((ushort)50, mismatch.RejectionCode);
        Assert.Contains("50", mismatch.Message, StringComparison.Ordinal);
        Assert.Contains("no common wire version", mismatch.Message, StringComparison.Ordinal);
        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task ShouldStopReconnectingGivenVersionMismatchWhenBrokerRejectsConnect()
    {
        // Arrange
        var attempts = 0;
        var failed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var first = new TestQueuedTransport();
        await using var rejected = new TestQueuedTransport { AutoHello = false };
        await using var unexpected = new TestQueuedTransport();
        using var payload = new BinaryBufferWriter();
        payload.WriteU16(50);
        payload.WriteString("no common wire version");
        rejected.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.ConnectRejected, payload.Build()));
        await using var client = new Client(new ClientConfig(
            new Uri("ws://queued/ws"),
            Reconnect: new ReconnectOptions(true, MaxAttempts: 3, Backoff: TimeSpan.FromMilliseconds(5), MaxBackoff: TimeSpan.FromMilliseconds(5)),
            Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt =>
            {
                if (evt.Event == "reconnect_failed")
                    failed.TrySetResult(true);
            }),
            TransportFactory: _ => Interlocked.Increment(ref attempts) switch { 1 => first, 2 => rejected, _ => unexpected }));
        await client.ConnectAsync();

        // Act
        first.QueueClosed();
        await failed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(100);

        // Assert
        Assert.Equal(2, Volatile.Read(ref attempts));
        Assert.Equal(ConnectionState.Disconnected, client.State);
    }
}
