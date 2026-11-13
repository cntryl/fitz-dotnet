using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerTests
{
    [Fact]
    public void ShouldRejectNotificationRegistrationGivenDisposedMultiplexerWhenDispatching()
    {
        // Arrange
        var mux = new Multiplexer();

        // Act
        mux.Dispose();

        // Assert
        Assert.Throws<ObjectDisposedException>(() => mux.RegisterNotificationHandler(90, _ => { }));
    }

    [Fact]
    public async Task ShouldReturnTypedErrorGivenDisposedMultiplexerWhenRequesting()
    {
        // Arrange
        var mux = new Multiplexer();

        // Act
        mux.Dispose();

        // Assert
        var error = await Assert.ThrowsAsync<ConnectionException>(() => mux.RequestAsync(
            90,
            [],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 1));

        Assert.Contains("closed", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ShouldRejectZeroCorrelationGivenConnectedMultiplexerWhenRequesting()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        // Act
        var act = () => mux.RequestAsync(
            90,
            [],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 0);

        // Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(act);
    }

    [Fact]
    public async Task ShouldBufferNotificationsGivenRestoreInProgressWhenDispatchingBeforeActivation()
    {
        // Arrange
        using var mux = new Multiplexer();
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = mux.RegisterNotificationHandler(90, payload => received.TrySetResult(payload.ToArray()));
        mux.BeginNotificationRestore();
        mux.SetConnected();

        // Act
        mux.DispatchNotification(90, new byte[] { 0xAB });

        // Assert
        Assert.False(received.Task.IsCompleted);

        mux.CompleteNotificationRestore();

        Assert.Equal([0xAB], await received.Task.WaitAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task ShouldResolveResponseGivenDispatchedMessageWhenRequesting()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var task = mux.RequestAsync(
            100,
            [1, 2, 3],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 7);

        // Act
        mux.DispatchCorrelated(7, new byte[] { 9, 8, 7 });

        // Assert
        var result = await task;
        Assert.Equal([9, 8, 7], result);
    }

    [Fact]
    public async Task ShouldThrowTimeoutGivenMissingDispatchWhenRequesting()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        // Act
        var act = () => mux.RequestAsync(
            101,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50),
            correlationId: 1);

        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(act);
    }

    [Fact]
    public async Task ShouldThrowOperationCanceledGivenCanceledTokenWhenRequesting()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var act = () => mux.RequestAsync(
            102,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 1,
            ct: cts.Token);

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
    }

    [Fact]
    public async Task ShouldDispatchToAllRegisteredHandlersGivenNotificationMessageWhenDispatching()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var seen = new List<string>();
        var dispatched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var first = mux.RegisterNotificationHandler(130, _ => seen.Add("first"));
        using var second = mux.RegisterNotificationHandler(130, _ =>
        {
            seen.Add("second");
            dispatched.TrySetResult();
        });

        // Act
        mux.DispatchNotification(130, new byte[] { 0x1 });
        await dispatched.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal(["first", "second"], seen);
    }

    [Fact]
    public async Task ShouldCancelInflightRequestGivenDisconnectWhenCancelAllIsCalled()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var inflight = mux.RequestAsync(
            121,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10),
            correlationId: 1);

        // Act
        mux.SetDisconnected();

        // Assert
        await Assert.ThrowsAsync<ConnectionException>(() => inflight);
    }

    [Fact]
    public async Task ShouldResolveSecondRequestGivenFirstRequestCanceledWhenSameMessageTypeIsInflight()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        using var firstCts = new CancellationTokenSource();
        var first = mux.RequestAsync(
            122,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            correlationId: 1,
            ct: firstCts.Token);

        var second = mux.RequestAsync(
            122,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            correlationId: 2);

        // Act
        await firstCts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        mux.DispatchCorrelated(1, new byte[] { 0xA });
        mux.DispatchCorrelated(2, new byte[] { 0xB });

        // Assert
        Assert.Equal([0xB], await second);
    }

    [Fact]
    public async Task ShouldAcceptReusedCorrelationGivenPriorDisconnectWhenFollowingRequestStarts()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var staleRequest = mux.RequestAsync(
            140,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(20),
            correlationId: 1);
        await Assert.ThrowsAsync<RequestTimeoutException>(() => staleRequest);
        mux.SetDisconnected();
        mux.SetConnected();

        // Act
        var nextRequest = mux.RequestAsync(
            140,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 1);
        mux.DispatchCorrelated(1, new byte[] { 0xB });

        // Assert
        Assert.Equal([0xB], await nextRequest);
    }

    [Fact]
    public async Task ShouldKeepOtherRequestsGivenCorrelatedRequestWhenTimeoutExpires()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var timedOut = mux.RequestAsync(
            141,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(20),
            correlationId: 1);
        var waiter = mux.RequestAsync(
            141,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            correlationId: 2);

        // Act
        await Assert.ThrowsAsync<RequestTimeoutException>(() => timedOut);
        var lateReply = Record.Exception(() => mux.DispatchCorrelated(1, new byte[] { 0xA }));
        mux.DispatchCorrelated(2, new byte[] { 0xB });

        // Assert
        Assert.Null(lateReply);
        Assert.Equal([0xB], await waiter);
    }

    [Fact]
    public async Task ShouldKeepRequestPastTimeoutGivenNonFinalFrameWhenFinalFrameArrivesLater()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var request = mux.RequestAsync(
            MessageTypes.LeaseAcquire,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(100),
            correlationId: 3);
        mux.DispatchCorrelated(3, new byte[] { 0, 2 }, responseFinal: false);

        // Act
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        var stillWaiting = !request.IsCompleted;
        mux.DispatchCorrelated(3, new byte[] { 0, 0, 0, 9 });

        // Assert
        Assert.True(stillWaiting, "A non-final frame must disarm the response timeout.");
        Assert.Equal([0, 0, 0, 9], await request.WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
