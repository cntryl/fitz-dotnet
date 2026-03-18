namespace Cntryl.Fitz.Core.Tests.Integration;

internal sealed record ScenarioResult(
    string ScenarioId,
    string Client,
    string Transport,
    string AuthMode,
    string Verdict,
    long LatencyMs,
    IReadOnlyList<string> Evidence,
    string Error
);

internal sealed record AggregateResult(
    string Suite,
    string Version,
    DateTimeOffset GeneratedAt,
    string Client,
    string Transport,
    string AuthMode,
    double P0PassRate,
    double P1PassRate,
    string OverallStatus,
    IReadOnlyList<ScenarioResult> Scenarios
);

internal static class ConformanceResultBuilder
{
    internal static AggregateResult BuildAggregate(IReadOnlyList<ScenarioResult> scenarios)
    {
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
        var overall = p0.Any(r => r.Verdict != "pass") ? "fail" : (p1.Any(r => r.Verdict is "partial" or "fail") ? "partial" : "pass");

        return new AggregateResult(
            "fitz-cross-language-client-conformance",
            "1.0",
            DateTimeOffset.UtcNow,
            "fitz-dotnet",
            "websocket",
            "anonymous",
            p0PassRate,
            p1PassRate,
            overall,
            scenarios
        );
    }
}
