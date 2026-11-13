using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ConnectionLossSignallingTests
{
    static readonly TimeSpan LockProbeBudget = TimeSpan.FromSeconds(5);

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

    [Fact]
    public async Task ShouldKeepRestoredSessionGivenSlowDisconnectListenerWhenTransportClosesDuringReconnect()
    {
        // Arrange
        await using var first = new TestQueuedTransport();
        await using var interrupted = new TestQueuedTransport();
        interrupted.AfterSend = sentFrameCount =>
        {
            // The broker accepts the session (HELLO is queued first) and then drops the connection.
            if (sentFrameCount == 1)
                interrupted.QueueClosed();
        };
        await using var restored = new TestQueuedTransport();
        var factoryCalls = 0;
        var connectionLosses = 0;
        var disconnects = 0;
        var restoredSession = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var interruptedLossReported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lossHandlerParked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseLossHandler = new ManualResetEventSlim();
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://queued/ws"),
            Reconnect: new ReconnectOptions(true, MaxAttempts: 3, Backoff: TimeSpan.Zero, MaxBackoff: TimeSpan.Zero),
            Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt =>
            {
                if (evt.Event == "reconnect_succeeded" && Volatile.Read(ref factoryCalls) >= 3)
                    restoredSession.TrySetResult();
                if (evt.Event == "connection_lost" && Interlocked.Increment(ref connectionLosses) == 2)
                    interruptedLossReported.TrySetResult();
            })), () => Interlocked.Increment(ref factoryCalls) switch
            {
                1 => first,
                2 => interrupted,
                _ => restored,
            });
        await connection.ConnectAsync();
        using var registration = connection.OnDisconnect(() =>
        {
            // The second disconnect belongs to the interrupted session. Hold its loss handler
            // here, partway through tearing that session down.
            if (Interlocked.Increment(ref disconnects) == 2)
            {
                lossHandlerParked.TrySetResult();
                releaseLossHandler.Wait(TimeSpan.FromSeconds(30));
            }
        });

        // Act
        try
        {
            first.QueueClosed();
            await lossHandlerParked.Task.WaitAsync(TimeSpan.FromSeconds(10));

            // A connection that does not wait for the parked handler restores a session within
            // this window; the handler's remaining steps then land on that newer session. The
            // window only makes the fault visible. It cannot fail a correct connection.
            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }
        finally
        {
            releaseLossHandler.Set();
        }

        await restoredSession.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await interruptedLossReported.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Assert
        Assert.Equal(ConnectionState.Authenticated, connection.State);
        Assert.Equal(3, Volatile.Read(ref factoryCalls));
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
        // A dedicated thread, not the pool: a starved pool on a busy runner must not read as a held lock.
        var contender = new Thread(() => connection.OnDisconnect(static () => { }).Dispose()) { IsBackground = true };
        contender.Start();
        return contender.Join(LockProbeBudget);
    }
}
