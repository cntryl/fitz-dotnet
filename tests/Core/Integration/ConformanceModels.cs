using System.Text.Json.Serialization;

namespace Cntryl.Fitz.Core.Tests.Integration;

internal sealed record ScenarioResult(
    [property: JsonPropertyName("scenario_id")] string ScenarioId,
    [property: JsonPropertyName("client")] string Client,
    [property: JsonPropertyName("transport")] string Transport,
    [property: JsonPropertyName("auth_mode")] string AuthMode,
    [property: JsonPropertyName("verdict")] string Verdict,
    [property: JsonPropertyName("latency_ms")] long LatencyMs,
    [property: JsonPropertyName("evidence")] IReadOnlyList<string> Evidence,
    [property: JsonPropertyName("notes")] string Notes
);

internal sealed record ConformanceSummary(
    [property: JsonPropertyName("client")] string Client,
    [property: JsonPropertyName("transport")] string Transport,
    [property: JsonPropertyName("auth_mode")] string AuthMode,
    [property: JsonPropertyName("total_scenarios")] int TotalScenarios,
    [property: JsonPropertyName("passed_scenarios")] int PassedScenarios,
    [property: JsonPropertyName("failed_scenarios")] int FailedScenarios,
    [property: JsonPropertyName("p0_pass_rate")] double P0PassRate,
    [property: JsonPropertyName("p1_pass_rate")] double P1PassRate,
    [property: JsonPropertyName("overall_status")] string OverallStatus
);

internal sealed record AggregateResult(
    [property: JsonPropertyName("client")] string Client,
    [property: JsonPropertyName("suite_version")] string SuiteVersion,
    [property: JsonPropertyName("run_started_at")] DateTimeOffset RunStartedAt,
    [property: JsonPropertyName("run_finished_at")] DateTimeOffset RunFinishedAt,
    [property: JsonPropertyName("scenarios")] IReadOnlyList<ScenarioResult> Scenarios,
    [property: JsonPropertyName("summary")] ConformanceSummary Summary
);

internal static class ConformanceResultBuilder
{
    internal static AggregateResult BuildAggregate(DateTimeOffset runStartedAt, DateTimeOffset runFinishedAt, IReadOnlyList<ScenarioResult> scenarios)
    {
        var client = scenarios.FirstOrDefault()?.Client ?? "fitz-dotnet";
        var transport = scenarios.FirstOrDefault()?.Transport ?? IntegrationFixture.GetConformanceTransport();
        var authMode = scenarios.FirstOrDefault()?.AuthMode ?? IntegrationFixture.GetConformanceAuthMode();
        var p0 = scenarios.Where(s => s.ScenarioId is "CS-001" or "CS-002" or "CS-003" or "CS-004" or "CS-005" or "CS-006" or "CS-007" or "CS-008").ToList();
        var p1 = scenarios.Where(s => s.ScenarioId is "CS-009" or "CS-010" or "CS-011" or "CS-012" or "CS-013" or "CS-014" or "CS-015").ToList();

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
        var passedScenarios = scenarios.Count(s => s.Verdict == "pass");
        var failedScenarios = scenarios.Count - passedScenarios;
        var overall = p0.Any(r => r.Verdict != "pass") ? "fail" : (p1.Any(r => r.Verdict is "partial" or "fail") ? "partial" : "pass");

        return new AggregateResult(
            client,
            "1.0",
            runStartedAt,
            runFinishedAt,
            scenarios,
            new ConformanceSummary(
                client,
                transport,
                authMode,
                scenarios.Count,
                passedScenarios,
                failedScenarios,
                p0PassRate,
                p1PassRate,
                overall
            )
        );
    }
}
