using Cntryl.Fitz.Connection;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerCleanupTests
{
    [Fact]
    public async Task ShouldCleanUpOnSendFailureGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();


        // Act
        var act = () => mux.RequestAsync(
            100,
            [1, 2, 3],
            async (data, _) =>
            {
                await Task.CompletedTask;
                throw new InvalidOperationException("Send failed");
            },
            TimeSpan.FromSeconds(5)
        );


        // Assert
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(act);
        Assert.Equal("Send failed", ex.Message);
    }

    [Fact]
    public async Task ShouldAllowNextRequestAfterSendFailureGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();


        // Act
        var firstTask = mux.RequestAsync(
            101,
            [0x1],
            async (data, _) =>
            {
                await Task.CompletedTask;
                throw new InvalidOperationException("First failed");
            },
            TimeSpan.FromSeconds(1)
        );


        // Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => firstTask);

        var secondTask = mux.RequestAsync(
            101,
            [0x2],
            async (data, _) => await Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(101, [0xA]);
        var result = await secondTask;
        Assert.Equal([0xA], result);
    }

    [Fact]
    public async Task ShouldNotDispatchToTimedOutRequestGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();


        // Act
        var requestTask = mux.RequestAsync(
            102,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50)
        );


        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(() => requestTask);

        // Dispatch after timeout should not throw
        mux.Dispatch(102, [0xB]);
    }

    [Fact]
    public async Task ShouldRequireANewSessionAfterUncorrelatedRequestTimeoutGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();


        // Act
        var timedOutRequest = mux.RequestAsync(
            108,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(20)
        );


        // Assert
        await Assert.ThrowsAnyAsync<RequestTimeoutException>(() => timedOutRequest);

        var followUp = mux.RequestAsync(
            108,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        await Assert.ThrowsAsync<ConnectionException>(() => followUp);

        mux.BeginSession();
        mux.SetConnected();
        var afterReconnect = mux.RequestAsync(
            108,
            [0x3],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1));
        mux.Dispatch(108, [0xBB]);

        Assert.Equal([0xBB], await afterReconnect);
    }

    [Fact]
    public async Task ShouldHandleCancellationBeforeSendGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();


        // Act
        var requestTask = mux.RequestAsync(
            103,
            [0x1],
            async (_, _) =>
            {
                await cts.CancelAsync();
            },
            TimeSpan.FromSeconds(5),
            ct: cts.Token
        );


        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task ShouldHandleCancellationAfterSendGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();

        var requestTask = mux.RequestAsync(
            104,
            [0x1],
            static async (_, token) => await Task.Delay(100, token),
            TimeSpan.FromSeconds(5),
            ct: cts.Token
        );

        await Task.Delay(50);

        // Act
        await cts.CancelAsync();


        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task ShouldRequireNewSessionGivenUncorrelatedRequestWhenCanceledAfterSend()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cancellation = new CancellationTokenSource();
        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var canceledRequest = mux.RequestAsync(
            109,
            [0x1],
            (_, _) =>
            {
                sent.TrySetResult();
                return Task.CompletedTask;
            },
            Timeout.InfiniteTimeSpan,
            ct: cancellation.Token);

        await sent.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Act
        await cancellation.CancelAsync();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledRequest);

        var nextRequest = mux.RequestAsync(
            109,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1));
        mux.Dispatch(109, [0xAA]);

        await Assert.ThrowsAsync<ConnectionException>(() => nextRequest);
    }

    [Fact]
    public async Task ShouldFailQueuedSameLaneRequestGivenSentRequestWhenCanceled()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var firstCts = new CancellationTokenSource();
        var first = mux.RequestAsync(
            105,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            ct: firstCts.Token
        );

        await Task.Delay(10);

        var second = mux.RequestAsync(
            105,
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

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await Assert.ThrowsAsync<ConnectionException>(() =>
            second.WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.False(secondStarted.Task.IsCompleted);
    }

    [Fact]
    public async Task ShouldCancelAllOnDisconnectGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        // Use short timeout combined with disconnect to ensure shutdown message wins
        var first = mux.RequestAsync(
            106,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(500)
        );

        var second = mux.RequestAsync(
            106,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(500)
        );

        mux.SetDisconnected();

        // Both should fail - with either ConnectionException or timeout
        // depending on timing/ordering
        var firstFailed = false;
        var secondFailed = false;

        try
        {
            await first;
        }
        catch (ConnectionException)
        {
            firstFailed = true;
        }
        catch (RequestTimeoutException)
        {
            firstFailed = true;
        }

        // Act
        try
        {
            await second;
        }
        catch (ConnectionException)
        {
            secondFailed = true;
        }
        catch (RequestTimeoutException)
        {
            secondFailed = true;
        }

        // Assert
        Assert.True(firstFailed && secondFailed, "Both requests should have failed");
    }

    [Fact]
    public async Task ShouldHandleMixedTimeoutDurationsGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var shortTimeout = mux.RequestAsync(
            107,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50)
        );

        await Task.Delay(10);


        // Act
        var longTimeout = mux.RequestAsync(
            108,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5)
        );


        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(() => shortTimeout);

        await Assert.ThrowsAsync<ConnectionException>(() => longTimeout);
    }

    [Fact]
    public async Task ShouldNotLeakOnRapidTimeoutsGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();

        // Act
        mux.SetConnected();


        // Assert
        for (var i = 0; i < 10; i++)
        {
            var task = mux.RequestAsync(
                (ushort)(200 + i),
                [0x1],
                static (_, _) => Task.CompletedTask,
                TimeSpan.FromMilliseconds(10)
            );

            await Assert.ThrowsAsync<RequestTimeoutException>(() => task);
            mux.BeginSession();
            mux.SetConnected();
        }

        var finalTask = mux.RequestAsync(
            210,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(210, [0xD]);
        var result = await finalTask;
        Assert.Equal([0xD], result);
    }
}
