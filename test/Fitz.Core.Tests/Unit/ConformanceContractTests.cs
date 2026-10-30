using System.Globalization;
using System.Text.Json;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ConformanceContractTests
{
    [Theory]
    [InlineData("P0", "fail")]
    [InlineData("P1", "partial")]
    public void ShouldNotPassGivenUnimplementedScenarioWhenBuildingAggregate(string priority, string expectedStatus)
    {
        // Arrange
        var missing = new Cntryl.Fitz.Core.Tests.Integration.ScenarioResult(
            "CS-020", "scenario", priority, "fitz-dotnet", "tcp", "anonymous", "not_implemented", 0, []);

        // Act
        var aggregate = Cntryl.Fitz.Core.Tests.Integration.ConformanceResultBuilder.BuildAggregate(
            "suite", "fitz-dotnet", "1.0", "tcp", "anonymous", DateTimeOffset.UtcNow, [missing]);

        // Assert
        Assert.Equal(expectedStatus, aggregate.OverallStatus);
    }

    [Fact]
    public void ShouldMatchSharedConformanceSuiteIdsGivenSharedContractWhenValidating()
    {
        // Arrange
        // Act
        // Assert
        var suite = Cntryl.Fitz.Core.Tests.Integration.ConformanceSmokeTests.ConformanceSuiteDefinition.Load(
            Cntryl.Fitz.Core.Tests.Integration.IntegrationFixture.GetConformanceSuitePath());

        Assert.Equal(24, suite.Scenarios.Count);
        Assert.Subset(
            suite.Scenarios.Select(scenario => scenario.ScenarioId).ToHashSet(StringComparer.Ordinal),
            Cntryl.Fitz.Core.Tests.Integration.ConformanceSmokeTests.ImplementedScenarioIds.ToHashSet(StringComparer.Ordinal));
    }

    [Fact]
    public void ShouldEmitSharedConformanceResultShapeGivenSharedContractWhenValidating()
    {
        // Arrange
        var aggregate = Cntryl.Fitz.Core.Tests.Integration.ConformanceResultBuilder.BuildAggregate(
            "fitz-cross-language-client-conformance",
            "fitz-dotnet",
            "1.0",
            "ws",
            "anonymous",
            DateTimeOffset.Parse("2026-07-09T00:00:00Z", CultureInfo.InvariantCulture),
            [
                new Cntryl.Fitz.Core.Tests.Integration.ScenarioResult(
                    "CS-001",
                    "connect success",
                    "P0",
                    "fitz-dotnet",
                    "ws",
                    "anonymous",
                    "pass",
                    12,
                    ["connect returned successfully"]),
                new Cntryl.Fitz.Core.Tests.Integration.ScenarioResult(
                    "CS-010",
                    "reconnect and retry behavior",
                    "P1",
                    "fitz-dotnet",
                    "ws",
                    "anonymous",
                    "pass",
                    24,
                    ["same-client reconnect succeeded"]),
            ]);

        using var document = JsonDocument.Parse(JsonSerializer.Serialize(aggregate));

        // Act
        var root = document.RootElement;


        // Assert
        Assert.Equal("fitz-cross-language-client-conformance", root.GetProperty("suite").GetString());
        Assert.Equal("1.0", root.GetProperty("version").GetString());
        Assert.Equal("fitz-dotnet", root.GetProperty("client").GetString());
        Assert.Equal("ws", root.GetProperty("transport").GetString());
        Assert.Equal("anonymous", root.GetProperty("auth_mode").GetString());
        Assert.True(root.TryGetProperty("generated_at", out _));
        Assert.Equal(1, root.GetProperty("p0_pass").GetInt32());
        Assert.Equal(1, root.GetProperty("p0_total").GetInt32());
        Assert.Equal(1, root.GetProperty("p1_pass").GetInt32());
        Assert.Equal(1, root.GetProperty("p1_total").GetInt32());
        Assert.True(root.TryGetProperty("p0_pass_rate", out _));
        Assert.True(root.TryGetProperty("p1_pass_rate", out _));
        Assert.True(root.TryGetProperty("overall_status", out _));
        Assert.False(root.TryGetProperty("summary", out _));

        var firstScenario = root.GetProperty("scenarios")[0];
        Assert.Equal("connect success", firstScenario.GetProperty("title").GetString());
        Assert.Equal("P0", firstScenario.GetProperty("priority").GetString());
        Assert.Equal(JsonValueKind.Array, firstScenario.GetProperty("evidence").ValueKind);
        Assert.False(firstScenario.TryGetProperty("notes", out _));
    }
}
