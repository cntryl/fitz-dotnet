using Cntryl.Fitz.Connection;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
    [Fact]
    public async Task ShouldAdvertiseServer020CapabilitiesGivenBrokerWhenConnected()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateClientForMode(
            IntegrationFixture.GetConformanceTransport(), IntegrationFixture.GetConformanceAuthMode());
        var config = client.Config;
        await using var connection = new FitzConnection(config, () => TransportResolver.Resolve(config));

        // Act
        await connection.ConnectAsync();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (connection.Capabilities.ProtocolVersion == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }
        var capabilities = connection.Capabilities;

        // Assert
        Assert.Equal(1, capabilities.ProtocolVersion);
        Assert.True(capabilities.SupportsCorrelation, "server 0.2.0 requires correlation");
        Assert.True(capabilities.SupportsSessionMetadata, "server 0.2.0 requires session metadata");
        Assert.True(capabilities.SupportsKvScanExclusive, "server 0.2.0 requires exclusive KV scan");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShouldExcludeResumeKeyGivenDirectionalKvScanWhenResumed(bool reverse)
    {
        // Arrange
        await using var client = IntegrationFixture.CreateClientForMode(
            IntegrationFixture.GetConformanceTransport(), IntegrationFixture.GetConformanceAuthMode());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("kv");
        await using var seed = await client.Kv.BeginAsync(route, KvDurability.Sync);
        foreach (var key in new byte[][] { [0x10], [0x10, 0], [0x20] })
        {
            await seed.PutAsync(key, key);
        }
        await seed.CommitAsync();
        await using var tx = await client.Kv.BeginAsync(route, KvDurability.Sync, KvMode.ReadOnly);
        var first = await tx.ScanAsync(new KvScanQuery(Limit: 1, Reverse: reverse));
        var start = Assert.Single(first.Pairs).Key;
        Assert.True(first.HasMore);

        // Act
        var resumed = await tx.ScanAsync(new KvScanQuery(StartKey: start, StartExclusive: true, Limit: 2, Reverse: reverse));

        // Assert
        var expected = reverse ? new byte[][] { [0x10, 0], [0x10] } : new byte[][] { [0x10, 0], [0x20] };
        Assert.Equal(expected, resumed.Pairs.Select(pair => pair.Key.ToArray()).ToArray());
        Assert.False(resumed.HasMore);
    }
}
