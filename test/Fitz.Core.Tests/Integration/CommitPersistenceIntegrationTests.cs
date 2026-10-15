namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class CommitPersistenceIntegrationTests
{
    [Theory]
    [InlineData("tcp", KvDurability.Buffered, StreamCommitMode.Buffered)]
    [InlineData("tcp", KvDurability.Sync, StreamCommitMode.Sync)]
    [InlineData("websocket", KvDurability.Buffered, StreamCommitMode.Buffered)]
    [InlineData("websocket", KvDurability.Sync, StreamCommitMode.Sync)]
    public async Task ShouldCommitGivenExplicitPersistenceWhenUsingDevelopBroker(string transport, KvDurability durability, StreamCommitMode streamMode)
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(
            IntegrationFixture.GetBrokerUrl(transport, "anonymous"), transport);
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("kv");
        await using var transaction = await client.Kv.BeginAsync(route);
        await transaction.PutAsync("key"u8.ToArray(), "value"u8.ToArray());
        await using var session = await client.Stream.BeginAsync(IntegrationFixture.CreateUniqueRoute("stream"));
        await session.AppendAsync(0, "record"u8.ToArray());

        // Act
        await transaction.CommitAsync(durability);
        await session.CommitAsync(streamMode);

        // Assert
        await using var read = await client.Kv.BeginAsync(route, KvMode.ReadOnly);
        var result = await read.GetAsync("key"u8.ToArray());
        Assert.True(result.Found);
        Assert.Equal("value"u8.ToArray(), result.Value!.Value.ToArray());
    }
}
