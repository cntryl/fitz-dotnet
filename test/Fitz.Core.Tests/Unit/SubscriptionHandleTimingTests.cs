namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SubscriptionHandleTimingTests
{
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ShouldAbandonUnsubscribeGivenStalledBrokerWhenCleanupBudgetElapses()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var unsubscribeStarted = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handle = new NoticeSubscription(
            "notice://realm/area/resource",
            ct =>
            {
                unsubscribeStarted.SetResult(ct);
                return new ValueTask(Task.Delay(Timeout.InfiniteTimeSpan, ct));
            },
            timeProvider: time);

        // Act
        var disposing = handle.DisposeAsync().AsTask();
        var unsubscribeToken = await unsubscribeStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        time.Advance(CleanupBudget - TimeSpan.FromTicks(1));
        var cancelledEarly = unsubscribeToken.IsCancellationRequested;
        var abandonedEarly = disposing.IsCompleted;
        time.Advance(TimeSpan.FromTicks(1));
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.False(cancelledEarly);
        Assert.False(abandonedEarly);
        Assert.True(unsubscribeToken.IsCancellationRequested);
    }
}
