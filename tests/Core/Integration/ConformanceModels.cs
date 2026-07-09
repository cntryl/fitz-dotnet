using System.Text.Json.Serialization;

namespace Cntryl.Fitz.Core.Tests.Integration;

internal sealed record ConformanceRunConfig(
    string SuitePath,
    string ClientName,
    string Transport,
    string AuthMode,
    string BrokerAddress,
    string OutputPath,
    double TimeoutScale,
    bool? ReconnectEnabledOverride,
    int? Seed
);

internal sealed record ScenarioResult(
    [property: JsonPropertyName("scenario_id")] string ScenarioId,
    [property: JsonPropertyName("title")] string Title,
    [property: JsonPropertyName("priority")] string Priority,
    [property: JsonPropertyName("client")] string Client,
    [property: JsonPropertyName("transport")] string Transport,
    [property: JsonPropertyName("auth_mode")] string AuthMode,
    [property: JsonPropertyName("verdict")] string Verdict,
    [property: JsonPropertyName("latency_ms")] long LatencyMs,
    [property: JsonPropertyName("evidence")] IReadOnlyList<string> Evidence,
    [property: JsonPropertyName("error")] string? Error = null
);

internal sealed record AggregateResult(
    [property: JsonPropertyName("suite")] string Suite,
    [property: JsonPropertyName("version")] string Version,
    [property: JsonPropertyName("generated_at")] DateTimeOffset GeneratedAt,
    [property: JsonPropertyName("client")] string Client,
    [property: JsonPropertyName("transport")] string Transport,
    [property: JsonPropertyName("auth_mode")] string AuthMode,
    [property: JsonPropertyName("p0_pass_rate")] double P0PassRate,
    [property: JsonPropertyName("p1_pass_rate")] double P1PassRate,
    [property: JsonPropertyName("overall_status")] string OverallStatus,
    [property: JsonPropertyName("scenarios")] IReadOnlyList<ScenarioResult> Scenarios
);

internal static class ConformanceResultBuilder
{
    internal static AggregateResult BuildAggregate(
        string suite,
        string client,
        string suiteVersion,
        string transport,
        string authMode,
        DateTimeOffset generatedAt,
        IReadOnlyList<ScenarioResult> scenarios)
    {
        var p0 = scenarios.Where(s => string.Equals(s.Priority, "P0", StringComparison.Ordinal)).ToList();
        var p1 = scenarios.Where(s => string.Equals(s.Priority, "P1", StringComparison.Ordinal)).ToList();

        static double PassRate(IEnumerable<ScenarioResult> input)
        {
            var arr = input.ToArray();
            if (arr.Length == 0)
            {
                return 1.0;
            }

            var pass = arr.Count(r => r.Verdict == "pass");
            return (double)pass / arr.Length;
        }

        var p0PassRate = PassRate(p0);
        var p1PassRate = PassRate(p1);
        var overall = p0.Any(r => r.Verdict != "pass") ? "fail" : (p1.Any(r => r.Verdict is "partial" or "fail") ? "partial" : "pass");

        return new AggregateResult(
            suite,
            suiteVersion,
            generatedAt,
            client,
            transport,
            authMode,
            p0PassRate,
            p1PassRate,
            overall,
            scenarios
        );
    }
}

internal static class ConformanceScenarioCatalog
{
    private static readonly Lazy<IReadOnlyDictionary<string, (string Title, string Priority)>> Definitions = new(() =>
    {
        var suite = ConformanceSmokeTests.ConformanceSuiteDefinition.Load(IntegrationFixture.GetConformanceSuitePath());
        return suite.Scenarios.ToDictionary(
            scenario => scenario.ScenarioId,
            scenario => (scenario.Title, scenario.Priority),
            StringComparer.Ordinal);
    });

    internal static (string Title, string Priority) Get(string scenarioId)
    {
        if (!Definitions.Value.TryGetValue(scenarioId, out var metadata))
        {
            throw new InvalidOperationException($"Missing conformance metadata for '{scenarioId}'.");
        }

        return metadata;
    }
}
