using Cntryl.Fitz.Connection;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
    [Fact]
    public async Task ShouldCorrelateSameTypeRequestsGivenOutOfOrderReplies()
    {
        // Arrange
        var transport = IntegrationFixture.GetConformanceTransport();
        var authMode = IntegrationFixture.GetConformanceAuthMode();

        // Act
        var result = await RunCs019SameTypeRequests(transport, authMode);

        // Assert
        Assert.Equal("pass", result.Verdict);
    }

    static async Task<ScenarioResult> RunCs019SameTypeRequests(string transport, string authMode)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();
        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();
            var parkedRoute = IntegrationFixture.CreateUniqueRoute("queue");
            var readyRoute = IntegrationFixture.CreateUniqueRoute("queue");
            await client.Queue.EnqueueAsync(readyRoute, "second"u8.ToArray());
            var parked = client.Queue.ReserveAsync(parkedRoute, lease: TimeSpan.FromSeconds(30), batchSize: 1, wait: TimeSpan.FromSeconds(5));
            await Task.Delay(100);
            Assert.False(parked.IsCompleted);
            var second = await client.Queue.ReserveAsync(readyRoute, lease: TimeSpan.FromSeconds(30), batchSize: 1, wait: TimeSpan.Zero).WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal("second"u8.ToArray(), Assert.Single(second).Body.ToArray());
            Assert.False(parked.IsCompleted);
            await client.Queue.EnqueueAsync(parkedRoute, "first"u8.ToArray());
            var first = await parked.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("first"u8.ToArray(), Assert.Single(first).Body.ToArray());
            evidence.Add("second reserve resolved first with its own payload");
            evidence.Add("parked first reserve resolved with its own payload");
            return Result("CS-019", transport, authMode, "pass", stopwatch.ElapsedMilliseconds, evidence);
        }
        catch (Exception error)
        {
            evidence.Add($"same-type requests failed: {error.GetType().Name}: {error.Message}");
            return Result("CS-019", transport, authMode, "fail", stopwatch.ElapsedMilliseconds, evidence, error.Message);
        }
    }

    static async Task<ScenarioResult> RunCs024StreamBoundary(string transport, string authMode)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();
        try
        {
            await StreamBoundariesTests.ExerciseBoundaryAsync(transport, authMode);
            evidence.Add("409600 combined bytes accepted; 409601 rejected with BAD_REQUEST/NOT_APPLIED");
            evidence.Add("three complete events replayed on a 4096-byte route across multiple pages");
            return Result("CS-024", transport, authMode, "pass", stopwatch.ElapsedMilliseconds, evidence);
        }
        catch (Exception error)
        {
            evidence.Add($"Stream boundary failed: {error.GetType().Name}: {error.Message}");
            return Result("CS-024", transport, authMode, "fail", stopwatch.ElapsedMilliseconds, evidence, error.Message);
        }
    }

    static async Task<ScenarioResult> RunCs020HandshakeNegotiation(string transport, string authMode)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();
        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            var config = client.Config;
            await using var connection = new FitzConnection(config, () => TransportResolver.Resolve(config));
            await connection.ConnectAsync();
            var capabilities = connection.Capabilities;
            Assert.Equal(2, capabilities.ProtocolVersion);
            Assert.Equal(0u, capabilities.CapabilityBits);
            Assert.True(capabilities.MaxMessageBytes > 0);
            Assert.True(capabilities.MaxInFlightRequests > 0);
            evidence.Add($"protocol_version={capabilities.ProtocolVersion}");
            evidence.Add($"broker_caps={capabilities.CapabilityBits}");
            evidence.Add($"max_message_bytes={capabilities.MaxMessageBytes}");
            evidence.Add($"max_in_flight={capabilities.MaxInFlightRequests}");
            return Result("CS-020", transport, authMode, "pass", stopwatch.ElapsedMilliseconds, evidence);
        }
        catch (Exception error)
        {
            evidence.Add($"handshake negotiation failed: {error.GetType().Name}: {error.Message}");
            return Result("CS-020", transport, authMode, "fail", stopwatch.ElapsedMilliseconds, evidence, error.Message);
        }
    }
}
