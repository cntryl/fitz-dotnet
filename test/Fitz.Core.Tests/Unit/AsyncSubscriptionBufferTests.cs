using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class AsyncSubscriptionBufferTests
{
    [Fact]
    public async Task ShouldTerminateOnlySlowBufferGivenCapacityExceededWhenWriting()
    {
        // Arrange
        var slow = new AsyncSubscriptionBuffer<int>("notice://realm/area/*", 1);
        var sibling = new AsyncSubscriptionBuffer<int>("notice://realm/area/*", 1);

        // Act
        slow.Write(1);
        Assert.Throws<SubscriptionBackpressureException>(() => slow.Write(2));
        sibling.Write(7);
        sibling.Complete();

        // Assert
        await using var slowEnumerator = slow.ReadAllAsync().GetAsyncEnumerator();
        Assert.True(await slowEnumerator.MoveNextAsync());
        await Assert.ThrowsAsync<SubscriptionBackpressureException>(
            () => slowEnumerator.MoveNextAsync().AsTask());

        var siblingValues = new List<int>();
        await foreach (var value in sibling.ReadAllAsync())
        {
            siblingValues.Add(value);
        }

        Assert.Equal([7], siblingValues);
    }
}
