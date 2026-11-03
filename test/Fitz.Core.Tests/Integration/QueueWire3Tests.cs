namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class QueueWire3Tests
{
    [Fact]
    public async Task ShouldRelinquishMessageGivenTtlWhenNackIsImmediate()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await client.Queue.EnqueueWithOptionsAsync(route, "wire3"u8.ToArray(), new QueueEnqueueOptions(TtlHours: 1));
        var first = Assert.Single(await client.Queue.ReserveAsync(route, TimeSpan.FromSeconds(30)));
        // Act
        await first.NackAsync();
        var second = await client.Queue.ReserveAsync(route, TimeSpan.FromSeconds(30));
        // Assert
        var item = Assert.Single(second);
        Assert.Equal("wire3"u8.ToArray(), item.Body.ToArray());
        await item.CompleteAsync();
    }

    [Fact]
    public async Task ShouldDeferVisibilityGivenDelayWhenNackSucceeds()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await client.Queue.EnqueueAsync(route, "delay"u8.ToArray());
        var item = Assert.Single(await client.Queue.ReserveAsync(route, TimeSpan.FromSeconds(30)));
        // Act
        await item.NackAsync(TimeSpan.FromSeconds(60));
        var early = await client.Queue.ReserveAsync(route, TimeSpan.FromSeconds(30));
        // Assert
        Assert.Empty(early);
    }
}
