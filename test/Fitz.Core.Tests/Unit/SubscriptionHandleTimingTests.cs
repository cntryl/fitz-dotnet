namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SubscriptionHandleTimingTests
{
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ShouldAbandonUnsubscribeGivenStalledBrokerWhenCleanupBudgetElapses()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var unsubscribeCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handle = new NoticeSubscription(
            "notice://realm/area/resource",
            async ct =>
            {
                await using (ct.Register(unsubscribeCancelled.SetResult))
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            },
            timeProvider: time);

        // Act
        var disposing = handle.DisposeAsync().AsTask();
        await time.WaitForTimerAsync(CleanupBudget);
        time.Advance(CleanupBudget - TimeSpan.FromTicks(1));
        var abandonedEarly = disposing.IsCompleted;
        time.Advance(TimeSpan.FromTicks(1));
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.False(abandonedEarly);
        Assert.True(unsubscribeCancelled.Task.IsCompleted);
    }
}
