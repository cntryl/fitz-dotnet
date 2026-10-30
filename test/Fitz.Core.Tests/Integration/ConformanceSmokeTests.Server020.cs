using Cntryl.Fitz.Connection;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
    [Fact]
    public async Task ShouldAdvertiseWire2CapabilitiesGivenBrokerWhenConnected()
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
        Assert.Equal(2, capabilities.ProtocolVersion);
        Assert.Equal(0u, capabilities.CapabilityBits);
        Assert.True(capabilities.SupportsCorrelation);
        Assert.False(Cntryl.Fitz.Protocol.ServerCapabilities.SupportsSessionMetadata);
        Assert.True(capabilities.SupportsKvScanExclusive);
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
        await using var seed = await client.Kv.BeginAsync(route);
        foreach (var key in new byte[][] { [0x10], [0x10, 0], [0x20] })
        {
            await seed.PutAsync(key, key);
        }
        await seed.CommitAsync(KvDurability.Sync);
        await using var tx = await client.Kv.BeginAsync(route, KvMode.ReadOnly);
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
