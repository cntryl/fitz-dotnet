using System.Text.Json;
using System.Globalization;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ConformanceContractTests
{
    [Fact]
    public void should_match_shared_conformance_suite_ids()
    {
        var suite = Cntryl.Fitz.Core.Tests.Integration.ConformanceSmokeTests.ConformanceSuiteDefinition.Load(
            Cntryl.Fitz.Core.Tests.Integration.IntegrationFixture.GetConformanceSuitePath());

        Assert.Equal(
            Cntryl.Fitz.Core.Tests.Integration.ConformanceSmokeTests.ImplementedScenarioIds,
            suite.Scenarios.Select(scenario => scenario.ScenarioId));
    }

    [Fact]
    public void should_emit_shared_conformance_result_shape()
    {
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
        var root = document.RootElement;

        Assert.Equal("fitz-cross-language-client-conformance", root.GetProperty("suite").GetString());
        Assert.Equal("1.0", root.GetProperty("version").GetString());
        Assert.Equal("fitz-dotnet", root.GetProperty("client").GetString());
        Assert.Equal("ws", root.GetProperty("transport").GetString());
        Assert.Equal("anonymous", root.GetProperty("auth_mode").GetString());
        Assert.True(root.TryGetProperty("generated_at", out _));
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
