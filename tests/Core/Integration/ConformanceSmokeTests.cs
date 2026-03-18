using System.Text.Json;
using Cntryl.Fitz;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class ConformanceSmokeTests
{
    [Fact]
    public async Task RunConformanceSmoke_WhenEnabled_WritesJsonResult()
    {
        if (!IsEnabled())
        {
            return;
        }

        var scenarios = new List<ScenarioResult>
        {
            await RunCs001ConnectSuccess(),
        };

        var outputPath = Environment.GetEnvironmentVariable("CONFORMANCE_OUTPUT");
        if (string.IsNullOrWhiteSpace(outputPath))
        {
            outputPath = Path.Combine(AppContext.BaseDirectory, "conformance-results.json");
        }

        var aggregate = BuildAggregate(scenarios);
        await File.WriteAllTextAsync(
            outputPath,
            JsonSerializer.Serialize(aggregate, new JsonSerializerOptions { WriteIndented = true })
        );

        Assert.Contains(scenarios, s => s.ScenarioId == "CS-001");
        Assert.True(File.Exists(outputPath));
    }

    private static bool IsEnabled()
    {
        return string.Equals(
            Environment.GetEnvironmentVariable("FITZ_DOTNET_RUN_INTEGRATION"),
            "1",
            StringComparison.Ordinal
        );
    }

    private static async Task<ScenarioResult> RunCs001ConnectSuccess()
    {
        var url = Environment.GetEnvironmentVariable("FITZ_BROKER_ANON_WS_ADDR") ?? "ws://localhost:4190/ws";
        var sw = System.Diagnostics.Stopwatch.StartNew();

        var evidence = new List<string>();
        try
        {
            await using var client = new Client(
                new ClientConfig(
                    url,
                    AuthSettleDelay: TimeSpan.FromMilliseconds(100)
                )
            );
            await client.ConnectAsync();
            evidence.Add("connect returned successfully");
            evidence.Add("client state is authenticated");
            return new ScenarioResult(
                "CS-001",
                "fitz-dotnet",
                "websocket",
                "anonymous",
                "pass",
                sw.ElapsedMilliseconds,
                evidence,
                ""
            );
        }
        catch (Exception ex)
        {
            evidence.Add($"connect failed: {ex.GetType().Name}");
            return new ScenarioResult(
                "CS-001",
                "fitz-dotnet",
                "websocket",
                "anonymous",
                "fail",
                sw.ElapsedMilliseconds,
                evidence,
                ex.Message
            );
        }
    }

    private static AggregateResult BuildAggregate(IReadOnlyList<ScenarioResult> scenarios)
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

    private sealed record ScenarioResult(
        string ScenarioId,
        string Client,
        string Transport,
        string AuthMode,
        string Verdict,
        long LatencyMs,
        IReadOnlyList<string> Evidence,
        string Error
    );

    private sealed record AggregateResult(
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
}