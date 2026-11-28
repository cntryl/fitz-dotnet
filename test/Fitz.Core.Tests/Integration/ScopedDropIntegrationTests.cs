namespace Cntryl.Fitz.Core.Tests.Integration;

[Trait("Category", "Integration")]
public sealed class ScopedDropIntegrationTests
{
    [Theory]
    [InlineData("tcp", 1, KvDurability.Sync)]
    [InlineData("websocket", 2, KvDurability.Buffered)]
    [InlineData("tcp", 3, KvDurability.Sync)]
    public async Task ShouldDeleteOnlySelectedKvDataGivenCommittedResources(string transport, int scope, KvDurability durability)
    {
        // Arrange
        await using var client = await ConnectAsync(transport);
        var (route, outside, selector) = CreateScope("kv", scope);
        await WriteKvAsync(client.Kv, route);
        await WriteKvAsync(client.Kv, outside);

        // Act
        await client.Kv.DropAsync(selector, durability);

        // Assert
        await using var deleted = await client.Kv.BeginAsync(route, KvMode.ReadOnly);
        await using var retained = await client.Kv.BeginAsync(outside, KvMode.ReadOnly);
        Assert.IsNotType<KvFound>((await deleted.GetAsync("key"u8.ToArray())).Value);
        Assert.IsType<KvFound>((await retained.GetAsync("key"u8.ToArray())).Value);
    }

    [Theory]
    [InlineData("websocket", 1, StreamCommitMode.Sync)]
    [InlineData("tcp", 2, StreamCommitMode.Buffered)]
    [InlineData("websocket", 3, StreamCommitMode.Sync)]
    public async Task ShouldResetOnlySelectedStreamsGivenCommittedResources(string transport, int scope, StreamCommitMode mode)
    {
        // Arrange
        await using var client = await ConnectAsync(transport);
        var (route, outside, selector) = CreateScope("stream", scope);
        await AppendAsync(client.Stream, route);
        await AppendAsync(client.Stream, outside);

        // Act
        await client.Stream.DropAsync(selector, mode);

        // Assert
        Assert.Empty(await ReplayAsync(client.Stream, route));
        Assert.Single(await ReplayAsync(client.Stream, outside));
        await AppendAsync(client.Stream, route);
        Assert.Equal(0UL, Assert.Single(await ReplayAsync(client.Stream, route)).Offset);
    }

    [Fact]
    public async Task ShouldRejectKvDropGivenAnOverlappingWriteTransaction()
    {
        // Arrange
        await using var client = await ConnectAsync("tcp");
        var (route, _, selector) = CreateScope("kv", 2);
        await using var write = await client.Kv.BeginAsync(route);

        // Act
        var failure = await Assert.ThrowsAsync<KvException>(() => client.Kv.DropAsync(selector, KvDurability.Sync));

        // Assert
        Assert.Equal(FitzErrorCodes.KvIsolationConflict, failure.DomainCode);
    }

    [Fact]
    public async Task ShouldRejectStreamDropGivenAnOverlappingAppendSession()
    {
        // Arrange
        await using var client = await ConnectAsync("websocket");
        var (route, _, selector) = CreateScope("stream", 2);
        await using var append = await client.Stream.BeginAsync(route);

        // Act
        var failure = await Assert.ThrowsAsync<StreamException>(() => client.Stream.DropAsync(selector, StreamCommitMode.Sync));

        // Assert
        Assert.Equal(FitzErrorCodes.StreamConcurrencyConflict, failure.DomainCode);
    }

    static async Task<FitzClient> ConnectAsync(string transport)
    {
        var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetBrokerUrl(transport, "anonymous"), transport);

        try
        {
            await client.ConnectAsync();
            return client;
        }
        catch
        {
            await client.DisposeAsync();
            throw;
        }
    }

    static (string Route, string Outside, string Selector) CreateScope(string scheme, int scope)
    {
        var realm = $"drop-{Guid.NewGuid():N}";
        var route = $"{scheme}://{realm}/app/data";
        return scope switch
        {
            1 => (route, $"{scheme}://{realm}-other/app/data", $"{scheme}://{realm}/*"),
            2 => (route, $"{scheme}://{realm}/other/data", $"{realm}/app/*"),
            _ => (route, $"{scheme}://{realm}/app/other", route),
        };
    }

    static async Task WriteKvAsync(IKvClient client, string route)
    {
        await using var write = await client.BeginAsync(route);
        await write.PutAsync("key"u8.ToArray(), "value"u8.ToArray());
        await write.CommitAsync(KvDurability.Sync);
    }

    static async Task<List<StreamRecord>> ReplayAsync(IStreamClient client, string route)
    {
        var records = new List<StreamRecord>();


        await foreach (var batch in client.ReadAsync(route, 0, StreamReadMode.Replay))
            records.AddRange(batch.Records);

        return records;
    }

    static async Task AppendAsync(IStreamClient client, string route)
    {
        await using var append = await client.BeginAsync(route);
        await append.AppendAsync(0, "record"u8.ToArray());
        await append.CommitAsync(StreamCommitMode.Sync);
    }
}
