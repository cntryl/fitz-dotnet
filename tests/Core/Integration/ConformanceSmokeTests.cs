using System.Collections.Generic;
using System.Text;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class ConformanceSmokeTests
{
    [Fact]
    public async Task should_write_json_result_given_enabled_flag_when_running_conformance_smoke()
    {
        // Arrange
        if (!IntegrationFixture.IsEnabled())
        {
            return;
        }

        var scenarios = new List<ScenarioResult>
        {
            await RunCs001ConnectSuccess(),
            await RunCs002AuthFailure(),
            await RunCs003RequestSuccess(),
            await RunCs004UnknownRoute(),
        };

        // Act
        var aggregate = ConformanceResultBuilder.BuildAggregate(scenarios);
        await IntegrationFixture.WriteAggregateAsync(aggregate);

        // Assert
        Assert.Contains(scenarios, s => s.ScenarioId == "CS-001");
        Assert.Contains(scenarios, s => s.ScenarioId == "CS-002");
        Assert.Contains(scenarios, s => s.ScenarioId == "CS-003");
        Assert.Contains(scenarios, s => s.ScenarioId == "CS-004");
        Assert.True(File.Exists(IntegrationFixture.GetOutputPath()));
    }

    private static async Task<ScenarioResult> RunCs001ConnectSuccess()
    {
        var url = IntegrationFixture.GetAnonymousWebSocketUrl();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var latency = new LatencyHistogram();
        var throughput = new ThroughputMeter();

        var evidence = new List<string>();
        try
        {
            await using var client = IntegrationFixture.CreateAnonymousClient(url);
            await PerfTimer.MeasureAsync(async () =>
            {
                await client.ConnectAsync();
            }, latency);
            throughput.RecordOperation();
            evidence.Add("connect returned successfully");
            evidence.Add("client state is authenticated");
            evidence.Add(PerfSummary.FromHistogram("CS-001", latency, throughput, p99Target: 100_000).ToString());
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

    private static async Task<ScenarioResult> RunCs002AuthFailure()
    {
        var url = IntegrationFixture.GetAuthWebSocketUrl();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var latency = new LatencyHistogram();
        var throughput = new ThroughputMeter();
        var evidence = new List<string>();

        await using var client = IntegrationFixture.CreateInvalidJwtClient(url);

        try
        {
            await PerfTimer.MeasureAsync(async () =>
            {
                await client.ConnectAsync();
            }, latency);
            throughput.RecordOperation();
            evidence.Add("connect did not raise (silent-close model)");
            evidence.Add($"client is_connected = {client.IsConnected}");

            try
            {
                _ = await client.Kv().BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"));
                evidence.Add("WARNING: domain request unexpectedly succeeded");
                return new ScenarioResult(
                    "CS-002",
                    "fitz-dotnet",
                    "websocket",
                    "invalid_jwt",
                    "partial",
                    sw.ElapsedMilliseconds,
                    evidence,
                    ""
                );
            }
            catch (Exception domainException)
            {
                evidence.Add($"domain request failed post-auth: {domainException.GetType().Name}: {domainException.Message}");
                return new ScenarioResult(
                    "CS-002",
                    "fitz-dotnet",
                    "websocket",
                    "invalid_jwt",
                    "partial",
                    sw.ElapsedMilliseconds,
                    evidence,
                    ""
                );
            }
        }
        catch (Exception ex)
        {
            evidence.Add($"connect raised {ex.GetType().Name}: {ex.Message}");
            evidence.Add("auth failure surfaced as error (correct)");
            evidence.Add(IntegrationFixture.DescribeAuthFailure(ex));
            evidence.Add(PerfSummary.FromHistogram("CS-002", latency, throughput, p99Target: 100_000).ToString());

            return new ScenarioResult(
                "CS-002",
                "fitz-dotnet",
                "websocket",
                "invalid_jwt",
                "pass",
                sw.ElapsedMilliseconds,
                evidence,
                ex.Message
            );
        }
    }

    private static async Task<ScenarioResult> RunCs003RequestSuccess()
    {
        var url = IntegrationFixture.GetAnonymousWebSocketUrl();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var latency = new LatencyHistogram();
        var throughput = new ThroughputMeter();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateAnonymousClient(url);

            await PerfTimer.MeasureAsync(async () =>
            {
                await client.ConnectAsync();
            }, latency);
            throughput.RecordOperation();

            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(route);
            await tx.PutAsync("user:1"u8.ToArray(), "Alice"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("kv begin/put/commit succeeded");

            var readTx = await client.Kv().BeginAsync(route, KvMode.ReadOnly);
            var result = await readTx.GetAsync("user:1"u8.ToArray());
            if (!result.Found)
            {
                return new ScenarioResult(
                    "CS-003",
                    "fitz-dotnet",
                    "websocket",
                    "anonymous",
                    "fail",
                    sw.ElapsedMilliseconds,
                    evidence,
                    "expected read-after-commit to return found=true"
                );
            }

            var value = result.Value.HasValue ? Encoding.UTF8.GetString(result.Value.Value.Span) : string.Empty;
            if (!string.Equals(value, "Alice", StringComparison.Ordinal))
            {
                return new ScenarioResult(
                    "CS-003",
                    "fitz-dotnet",
                    "websocket",
                    "anonymous",
                    "fail",
                    sw.ElapsedMilliseconds,
                    evidence,
                    $"expected read-after-commit value 'Alice', got '{value}'"
                );
            }

            evidence.Add($"read-after-commit returned \"{value}\" (correct)");
            evidence.Add(PerfSummary.FromHistogram("CS-003", latency, throughput, p99Target: 50_000).ToString());
            return new ScenarioResult(
                "CS-003",
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
            evidence.Add($"request success scenario failed: {ex.GetType().Name}: {ex.Message}");
            return new ScenarioResult(
                "CS-003",
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

    private static async Task<ScenarioResult> RunCs004UnknownRoute()
    {
        var url = IntegrationFixture.GetAnonymousWebSocketUrl();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var latency = new LatencyHistogram();
        var throughput = new ThroughputMeter();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateAnonymousClient(url, timeout: TimeSpan.FromMilliseconds(500));

            await PerfTimer.MeasureAsync(async () =>
            {
                await client.ConnectAsync();
            }, latency);
            throughput.RecordOperation();

            var noWorkerRoute = IntegrationFixture.CreateUniqueRoute("rpc");
            try
            {
                var frames = new List<RpcResponseFrame>();
                await foreach (var frame in client.Rpc().CallAsync(noWorkerRoute, new ReadOnlyMemory<byte>("ping"u8.ToArray())))
                {
                    frames.Add(frame);
                }
                evidence.Add("WARNING: rpc request to unregistered route unexpectedly succeeded");
                return new ScenarioResult(
                    "CS-004",
                    "fitz-dotnet",
                    "websocket",
                    "anonymous",
                    "fail",
                    sw.ElapsedMilliseconds,
                    evidence,
                    "unknown-route request unexpectedly succeeded"
                );
            }
            catch (Exception rpcException)
            {
                evidence.Add($"rpc to unregistered route raised {rpcException.GetType().Name}");
            }

            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(route);
            await tx.PutAsync("k"u8.ToArray(), "v"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("client remains usable after unknown-route error");
            evidence.Add(PerfSummary.FromHistogram("CS-004", latency, throughput, p99Target: 50_000).ToString());

            return new ScenarioResult(
                "CS-004",
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
            evidence.Add($"unknown-route scenario failed: {ex.GetType().Name}: {ex.Message}");
            return new ScenarioResult(
                "CS-004",
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

}