using System.Threading.Channels;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SubscriptionDispatchTests
{
    [Fact]
    public async Task ShouldCancelQueuedCallbackGivenActiveSubscriptionWhenDispatcherCloses()
    {
        // Arrange
        var firstChannel = Channel.CreateUnbounded<int>();
        var secondChannel = Channel.CreateUnbounded<int>();
        using var firstRegistration = new SubscriptionRegistration<int>(firstChannel);
        using var secondRegistration = new SubscriptionRegistration<int>(secondChannel);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondQueued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new AsyncHandlerDispatcher(
            maxConcurrency: 1,
            Timeout.InfiniteTimeSpan,
            queueCapacity: 1,
            _ => { },
            onMetricsChanged: (_, queued) =>
            {
                if (queued == 1)
                {
                    secondQueued.TrySetResult();
                }
            });

        SubscriptionPump.Start(firstRegistration, async (_, cancellationToken) =>
        {
            firstStarted.TrySetResult();
            await releaseFirst.Task.WaitAsync(cancellationToken);
        }, dispatcher.TryDispatch);
        SubscriptionPump.Start(secondRegistration, (_, _) => ValueTask.CompletedTask, dispatcher.TryDispatch);

        firstChannel.Writer.TryWrite(1);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        secondChannel.Writer.TryWrite(2);
        await secondQueued.Task.WaitAsync(TimeSpan.FromSeconds(1));


        // Act
        dispatcher.Close();


        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            secondRegistration.Completion.WaitAsync(TimeSpan.FromSeconds(1)));
        releaseFirst.TrySetResult();
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldFailCompletionAndCleanupGivenDispatchQueueOverflowWhenDispatching()
    {
        // Arrange
        var channel = Channel.CreateUnbounded<int>();
        var cleanupCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = new SubscriptionRegistration<int>(
            channel,
            "schedule",
            "schedule://realm/area/job/run",
            _ =>
            {
                cleanupCalled.TrySetResult();
                return ValueTask.CompletedTask;
            });
        SubscriptionPump.Start(registration, (_, _) => ValueTask.CompletedTask, (_, _) => false);


        // Act
        channel.Writer.TryWrite(1);


        // Assert
        var error = await Assert.ThrowsAsync<AsyncHandlerOverflowException>(
            () => registration.Completion.WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal(AsyncHandlerOverflowException.ErrorCode, error.Code);
        Assert.Equal("schedule", error.Domain);
        Assert.Equal("schedule://realm/area/job/run", error.Subscription);
        await cleanupCalled.Task.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldReturnFromStartGivenMessageAlreadyQueuedWhenPumpBegins()
    {
        // Arrange
        // Act
        // Assert
        var channel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        using var registration = new SubscriptionRegistration<int>(channel);
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        channel.Writer.TryWrite(1);

        var startTask = Task.Run(() =>
            SubscriptionPump.Start(registration, async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.WaitAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
            }));

        await startTask.WaitAsync(TimeSpan.FromSeconds(1));
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        releaseHandler.TrySetResult();
    }

    [Fact]
    public async Task ShouldCancelHandlerTokenGivenRegistrationDisposedWhileHandlerIsRunningWhenDispatching()
    {
        // Arrange
        var channel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        using var registration = new SubscriptionRegistration<int>(channel);
        var handlerStarted = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        SubscriptionPump.Start(registration, async (_, cancellationToken) =>
        {
            handlerStarted.TrySetResult(cancellationToken);
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                handlerCanceled.TrySetResult();
            }
        });

        channel.Writer.TryWrite(1);


        // Act
        var handlerToken = await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.NotEqual(default, handlerToken);
        Assert.False(handlerToken.IsCancellationRequested);

        registration.Dispose();

        await handlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldSkipQueuedMessagesGivenRegistrationDisposedBeforeNextHandlerRunsWhenDispatching()
    {
        // Arrange
        var channel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        using var registration = new SubscriptionRegistration<int>(channel);
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handledMessages = new List<int>();

        SubscriptionPump.Start(registration, async (message, _) =>
        {
            lock (handledMessages)
            {
                handledMessages.Add(message);
            }

            if (message == 1)
            {
                firstHandlerStarted.TrySetResult();
                await releaseFirstHandler.Task.WaitAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
            }
        });

        channel.Writer.TryWrite(1);
        await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        channel.Writer.TryWrite(2);
        registration.Dispose();
        releaseFirstHandler.TrySetResult();


        // Act
        await Task.Delay(100);


        // Assert
        lock (handledMessages)
        {
            Assert.Equal([1], handledMessages);
        }
    }
}
