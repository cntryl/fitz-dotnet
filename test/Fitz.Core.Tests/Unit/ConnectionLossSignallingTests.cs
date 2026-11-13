using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ConnectionLossSignallingTests
{
    static readonly TimeSpan LockProbeBudget = TimeSpan.FromSeconds(1);

    [Fact]
    public async Task ShouldReleaseConnectionLockGivenSessionTokenCallbackWhenConnectionIsLost()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(NoReconnectConfig(), () => transport);
        await connection.ConnectAsync();
        var probe = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = connection.ConnectionClosedToken.Register(
            () => probe.TrySetResult(CanTakeConnectionLockFromAnotherThread(connection)));

        // Act
        transport.QueueClosed();
        var lockWasFree = await probe.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(lockWasFree);
    }

    [Fact]
    public async Task ShouldReleaseConnectionLockGivenDisconnectListenerWhenConnectionIsLost()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(NoReconnectConfig(), () => transport);
        await connection.ConnectAsync();
        var probe = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = connection.OnDisconnect(
            () => probe.TrySetResult(CanTakeConnectionLockFromAnotherThread(connection)));

        // Act
        transport.QueueClosed();
        var lockWasFree = await probe.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(lockWasFree);
    }

    [Fact]
    public async Task ShouldDisposeRetiredSessionTokenSourceGivenReconnectWhenSessionIsReplaced()
    {
        // Arrange
        await using var first = new TestQueuedTransport();
        await using var second = new TestQueuedTransport();
        var factoryCalls = 0;
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://queued/ws"),
            Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(1), MaxBackoff: TimeSpan.FromMilliseconds(1)),
            Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt =>
            {
                if (evt.Event == "reconnect_succeeded")
                    reconnected.TrySetResult();
            })), () => Interlocked.Increment(ref factoryCalls) == 1 ? first : second);
        await connection.ConnectAsync();
        var retiredSession = connection.ConnectionClosedToken;

        // Act
        first.QueueClosed();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(3));

        // Assert
        Assert.True(retiredSession.IsCancellationRequested);
        Assert.Throws<ObjectDisposedException>(() => retiredSession.WaitHandle);
        Assert.False(connection.ConnectionClosedToken.IsCancellationRequested);
    }

    [Theory]
    [InlineData((ushort)8)]
    [InlineData((ushort)51)]
    public async Task ShouldSurfaceProtocolRejectionGivenNonCredentialCodeWhenBrokerRejectsConnect(ushort code)
    {
        // Arrange
        await using var transport = new TestQueuedTransport { AutoHello = false };
        transport.QueueIncomingFrame(ConnectRejectedFrame(code, "refused"));
        await using var connection = new FitzConnection(NoReconnectConfig(), () => transport);

        // Act
        var error = await Record.ExceptionAsync(() => connection.ConnectAsync());

        // Assert
        var rejection = Assert.IsType<ProtocolException>(error);
        Assert.Equal(code, rejection.RejectionCode);
        Assert.Equal(ConnectionState.Disconnected, connection.State);
    }

    [Fact]
    public async Task ShouldSurfaceAuthenticationRejectionGivenUnauthorizedCodeWhenBrokerRejectsConnect()
    {
        // Arrange
        await using var transport = new TestQueuedTransport { AutoHello = false };
        transport.QueueIncomingFrame(ConnectRejectedFrame(1, "token expired"));
        await using var connection = new FitzConnection(NoReconnectConfig(), () => transport);

        // Act
        var error = await Record.ExceptionAsync(() => connection.ConnectAsync());

        // Assert
        var rejection = Assert.IsType<AuthenticationException>(error);
        Assert.Equal((ushort)1, rejection.RejectionCode);
        Assert.Contains("token expired", rejection.Message, StringComparison.Ordinal);
    }

    static ClientConfig NoReconnectConfig() =>
        new(new Uri("ws://queued/ws"), Reconnect: new ReconnectOptions(Enabled: false));

    static byte[] ConnectRejectedFrame(ushort code, string message)
    {
        using var payload = new BinaryBufferWriter();
        payload.WriteU16(code);
        payload.WriteString(message);
        return FrameCodec.Encode(MessageTypes.ConnectRejected, payload.Build());
    }

    // Runs on the thread delivering the callback. Another thread registers a listener, which
    // needs the connection lock; if the callback is running under that lock, it cannot finish.
    static bool CanTakeConnectionLockFromAnotherThread(FitzConnection connection)
    {
        var contender = Task.Run(() => connection.OnDisconnect(static () => { }).Dispose());
        return SpinWait.SpinUntil(() => contender.IsCompleted, LockProbeBudget);
    }
}
