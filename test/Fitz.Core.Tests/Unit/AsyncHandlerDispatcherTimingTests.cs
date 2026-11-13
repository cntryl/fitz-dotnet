using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class AsyncHandlerDispatcherTimingTests
{
    [Fact]
    public async Task ShouldCancelHandlerAndReportTimeoutGivenManualClockWhenTimeoutElapses()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var timeout = TimeSpan.FromSeconds(30);
        var reported = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new AsyncHandlerDispatcher(
            maxConcurrency: 1,
            timeout,
            queueCapacity: 0,
            error => reported.TrySetResult(error),
            timeProvider: time);
        var accepted = dispatcher.TryDispatch(async ct =>
        {
            started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        });
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await time.WaitForTimerAsync(timeout);

        // Act
        time.Advance(timeout - TimeSpan.FromTicks(1));
        var timedOutEarly = reported.Task.IsCompleted;
        time.Advance(TimeSpan.FromTicks(1));
        var error = await reported.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(accepted);
        Assert.False(timedOutEarly);
        Assert.IsType<TimeoutException>(error);
        await dispatcher.DrainAsync().WaitAsync(TimeSpan.FromSeconds(5));
    }
}
