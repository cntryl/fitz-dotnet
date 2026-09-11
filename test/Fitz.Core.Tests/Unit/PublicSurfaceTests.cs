using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class PublicSurfaceTests
{
    [Fact]
    public async Task ShouldAllowRetryGivenUnsubscribeFailure()
    {
        var expected = new InvalidOperationException("unsubscribe failed");
        var attempts = 0;
        await using var handle = new TestSubscriptionHandle(_ =>
        {
            attempts++;
            return attempts == 1 ? ValueTask.FromException(expected) : ValueTask.CompletedTask;
        });

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => handle.UnsubscribeAsync().AsTask());
        await handle.UnsubscribeAsync();
        await handle.Completion;

        Assert.Same(expected, thrown);
        Assert.Equal(2, attempts);
    }

    sealed class TestSubscriptionHandle(Func<CancellationToken, ValueTask> unsubscribe)
        : SubscriptionHandle("notice://realm/area/resource", unsubscribe)
    {
    }
}
