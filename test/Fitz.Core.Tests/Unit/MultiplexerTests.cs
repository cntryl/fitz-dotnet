using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;

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
            TimeSpan.FromSeconds(1)));

        Assert.Contains("closed", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ShouldBufferNotificationsGivenRestoreInProgressWhenDispatchingBeforeActivation()
    {
        // Arrange
        using var mux = new Multiplexer();
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = mux.RegisterNotificationHandler(90, payload => received.TrySetResult(payload));
        mux.BeginNotificationRestore();
        mux.SetConnected();


        // Act
        mux.Dispatch(90, [0xAB]);

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
            static (data, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        // Act
        mux.Dispatch(100, [9, 8, 7]);

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
            static (data, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50)
        );

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
            cancellationToken: cts.Token
        );

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
    }

    [Fact]
    public async Task ShouldSerializeSameMessageTypeGivenTwoRequestsWhenDispatching()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var sendOrder = new List<byte>();
        var firstTask = mux.RequestAsync(
            120,
            [0x1],
            async (_, token) =>
            {
                sendOrder.Add(0x1);
                await Task.Delay(50, token);
            },
            TimeSpan.FromSeconds(1)
        );

        await Task.Delay(10);

        var secondTask = mux.RequestAsync(
            120,
            [0x2],
            (_, _) =>
            {
                sendOrder.Add(0x2);
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(1)
        );

        await Task.Delay(60);

        // Act
        mux.Dispatch(120, [0xA]);
        await Task.Delay(25);
        mux.Dispatch(120, [0xB]);

        // Assert
        Assert.Equal([0x1, 0x2], sendOrder);
        Assert.Equal([0xA], await firstTask);
        Assert.Equal([0xB], await secondTask);
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
        mux.Dispatch(130, [0x1]);
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
            TimeSpan.FromSeconds(10)
        );

        // Act
        mux.SetDisconnected();

        // Assert
        await Assert.ThrowsAsync<ConnectionException>(() => inflight);
    }

    [Fact]
    public async Task ShouldDispatchToNextRequestGivenFirstRequestCanceledWhenSameMessageTypeIsInflight()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var firstCts = new CancellationTokenSource();
        var first = mux.RequestAsync(
            122,
            [0x1],
            static async (_, token) =>
            {
                await Task.Delay(100, token);
            },
            TimeSpan.FromSeconds(5),
            cancellationToken: firstCts.Token
        );

        var second = mux.RequestAsync(
            122,
            [0x2],
            (_, _) =>
            {
                secondStarted.TrySetResult();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(5)
        );

        // Act
        await firstCts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await secondStarted.Task;
        mux.Dispatch(122, [0xB]);

        // Assert
        Assert.Equal([0xB], await second);
    }

    [Fact]
    public async Task ShouldNotDeliverMatcherRejectedResponseToUncorrelatedRequestGivenActiveMultiplexerWhenDispatching()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var firstSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var firstTask = mux.RequestAsync(
            130,
            [0x01],
            async (_, token) =>
            {
                firstSendStarted.TrySetResult();
                await Task.Delay(10, token);
            },
            TimeSpan.FromSeconds(1)
        );

        await firstSendStarted.Task;

        var secondTask = mux.RequestAsync(
            130,
            [0x02],
            (_, _) =>
            {
                secondSendStarted.TrySetResult();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(1),
            responseMatcher: response => response.Length > 0 && response.Span[0] == 2
        );

        await secondSendStarted.Task;

        // Act
        mux.Dispatch(130, [0xA]);
        await Task.Delay(20);

        // Assert
        Assert.False(firstTask.IsCompleted);

        mux.Dispatch(130, [2]);
        Assert.Equal([2], await secondTask);

        mux.Dispatch(130, [0xB]);
        Assert.Equal([0xB], await firstTask);
    }

    [Fact]
    public async Task ShouldMatchResponsesToCorrelatedRequestsGivenSameMessageTypeWhenDispatching()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var firstSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var firstTask = mux.RequestAsync(
            131,
            [0x01],
            async (_, token) =>
            {
                firstSendStarted.TrySetResult();
                await Task.Delay(10, token);
            },
            TimeSpan.FromSeconds(1),
            responseMatcher: response => response.Length > 0 && response.Span[0] == 1
        );

        await firstSendStarted.Task;

        var secondTask = mux.RequestAsync(
            131,
            [0x02],
            (_, _) =>
            {
                secondSendStarted.TrySetResult();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(1),
            responseMatcher: response => response.Length > 0 && response.Span[0] == 2
        );

        await secondSendStarted.Task;

        // Act
        mux.Dispatch(131, [2]);
        mux.Dispatch(131, [1]);

        // Assert
        Assert.Equal([1], await firstTask);
        Assert.Equal([2], await secondTask);
    }

    [Fact]
    public async Task ShouldDeliverUnmatchedResponsesToNotificationHandlersGivenMatchingByCorrelationFailsWhenDispatching()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var notifications = new List<byte[]>();
        using var _ = mux.RegisterNotificationHandler(132, payload =>
        {
            notifications.Add(payload.ToArray());
        });

        var request = mux.RequestAsync(
            132,
            [0x01],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            responseMatcher: payload => payload.Length > 0 && payload.Span[0] == 0x99
        );

        mux.Dispatch(132, [0x01]);

        await Task.Delay(15);
        Assert.NotNull(notifications);
        Assert.Single(notifications);
        Assert.Equal([0x01], notifications[0]);

        // Act
        mux.Dispatch(132, [0x99]);

        // Assert
        Assert.Equal([0x99], await request);
        Assert.Single(notifications);
    }

    [Fact]
    public async Task ShouldIgnoreStaleResponseGivenPriorDisconnectWhenFollowingRequestStarts()
    {
        // Arrange
        // Act
        // Assert
        using var mux = new Multiplexer();
        mux.SetConnected();

        var staleRequest = mux.RequestAsync(
            140,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(20)
        );

        await Assert.ThrowsAsync<RequestTimeoutException>(() => staleRequest);

        mux.SetDisconnected();
        mux.Dispatch(140, [0xAA]);

        mux.SetConnected();
        var nextRequest = mux.RequestAsync(
            140,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(140, [0xB]);

        // Assert
        Assert.Equal([0xB], await nextRequest);
    }

    [Fact]
    public async Task ShouldResetConnectionAndFailWaitersGivenUncorrelatedRequestWhenTimeoutExpires()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var timedOut = mux.RequestAsync(
            141,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(20));

        // Act
        var waiter = mux.RequestAsync(
            141,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(2));


        // Assert
        var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() => timedOut);
        var reset = await Assert.ThrowsAsync<ConnectionException>(() => waiter);

        Assert.Contains("lane was reset", timeout.Message, StringComparison.Ordinal);
        Assert.Contains("reset", reset.Message, StringComparison.Ordinal);
    }
}
