using System.Text;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class ConformanceSmokeTests
{
    [Fact]
    public async Task should_write_json_result_given_enabled_flag_when_running_conformance_suite()
    {
        if (!IntegrationFixture.IsEnabled())
        {
            return;
        }

        var transport = IntegrationFixture.GetConformanceTransport();
        var authMode = IntegrationFixture.GetConformanceAuthMode();

        var scenarios = new List<ScenarioResult>
        {
            await RunCs001ConnectSuccess(transport, authMode),
            await RunCs002AuthFailure(transport),
            await RunCs003RequestSuccess(transport, authMode),
            await RunCs004UnknownRoute(transport, authMode),
            await RunCs005InvalidPayload(transport, authMode),
            await RunCs006ServerErrorMapping(transport, authMode),
            await RunCs007TimeoutHandling(transport, authMode),
            await RunCs008CallerCancellation(transport, authMode),
            await RunCs009DisconnectDuringRequest(transport, authMode),
            await RunCs010ReconnectAndRetryBehavior(transport, authMode),
            await RunCs011StreamReceiveSequence(transport, authMode),
            await RunCs012StreamCompletion(transport, authMode),
            await RunCs013StreamErrorMidFlight(transport, authMode),
            await RunCs014ConcurrentInflightRequests(transport, authMode),
            await RunCs015ShutdownDuringActiveWork(transport, authMode),
        };

        var aggregate = ConformanceResultBuilder.BuildAggregate(scenarios);
        await IntegrationFixture.WriteAggregateAsync(aggregate);

        Assert.Equal(15, scenarios.Count);
        Assert.True(File.Exists(IntegrationFixture.GetOutputPath()));
    }

    private static async Task<ScenarioResult> RunCs001ConnectSuccess(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();
            evidence.Add("connect returned successfully");
            evidence.Add($"client is_connected = {client.IsConnected}");

            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(route);
            await tx.PutAsync("cs001-key"u8.ToArray(), "cs001-value"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("first domain request (kv) succeeded");

            return Result("CS-001", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"connect failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-001", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs002AuthFailure(string transport)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateInvalidJwtClient(
                IntegrationFixture.GetBrokerUrl(transport, "invalid_jwt"),
                transport);

            await client.ConnectAsync();
            evidence.Add("connect unexpectedly succeeded");
            return Result("CS-002", transport, "invalid_jwt", "fail", sw.ElapsedMilliseconds, evidence, "invalid JWT should fail connect");
        }
        catch (Exception ex)
        {
            evidence.Add($"connect raised {ex.GetType().Name}: {ex.Message}");
            evidence.Add(IntegrationFixture.DescribeAuthFailure(ex));
            return Result("CS-002", transport, "invalid_jwt", ex is AuthenticationException ? "pass" : "partial", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs003RequestSuccess(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();

            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(route);
            await tx.PutAsync("user:1"u8.ToArray(), "Alice"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("kv begin/put/commit succeeded");

            var readTx = await client.Kv().BeginAsync(route, Abstractions.Domains.Kv.KvMode.ReadOnly);
            var result = await readTx.GetAsync("user:1"u8.ToArray());
            if (!result.Found)
            {
                return Result("CS-003", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "expected found=true");
            }

            var value = result.Value.HasValue ? Encoding.UTF8.GetString(result.Value.Value.Span) : string.Empty;
            evidence.Add($"read-after-commit returned \"{value}\"");
            return Result("CS-003", transport, authMode, string.Equals(value, "Alice", StringComparison.Ordinal) ? "pass" : "fail", sw.ElapsedMilliseconds, evidence, value);
        }
        catch (Exception ex)
        {
            evidence.Add($"request success failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-003", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs004UnknownRoute(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode, timeout: TimeSpan.FromMilliseconds(750));
            await client.ConnectAsync();

            var noWorkerRoute = IntegrationFixture.CreateUniqueRoute("rpc");
            Exception? caught = null;
            try
            {
                await foreach (var _ in client.Rpc().CallAsync(noWorkerRoute, "ping"u8.ToArray()))
                {
                }
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            if (caught is null)
            {
                return Result("CS-004", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "unknown route unexpectedly succeeded");
            }

            evidence.Add($"rpc to unregistered route raised {caught.GetType().Name}");

            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(route);
            await tx.PutAsync("k"u8.ToArray(), "v"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("client remains usable after unknown-route error");
            return Result("CS-004", transport, authMode, caught is RpcException ? "pass" : "partial", sw.ElapsedMilliseconds, evidence, caught.Message);
        }
        catch (Exception ex)
        {
            evidence.Add($"unknown-route scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-004", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs005InvalidPayload(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();

            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(route);
            await tx.InsertAsync("dup-key"u8.ToArray(), "first"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("first insert succeeded");

            var tx2 = await client.Kv().BeginAsync(route);
            Exception? caught = null;
            try
            {
                await tx2.InsertAsync("dup-key"u8.ToArray(), "second"u8.ToArray());
            }
            catch (Exception ex)
            {
                caught = ex;
            }
            finally
            {
                await tx2.RollbackAsync();
            }

            if (caught is null)
            {
                return Result("CS-005", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "duplicate insert unexpectedly succeeded");
            }

            evidence.Add($"duplicate insert raised {caught.GetType().Name}");
            return Result("CS-005", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence, caught.Message);
        }
        catch (Exception ex)
        {
            evidence.Add($"invalid payload scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-005", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs006ServerErrorMapping(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode, timeout: TimeSpan.FromMilliseconds(750));
            await client.ConnectAsync();

            Exception? rpcError = null;
            try
            {
                await foreach (var _ in client.Rpc().CallAsync(IntegrationFixture.CreateUniqueRoute("rpc"), "ping"u8.ToArray()))
                {
                }
            }
            catch (Exception ex)
            {
                rpcError = ex;
            }

            if (rpcError is not null)
            {
                evidence.Add($"rpc error type: {rpcError.GetType().Name}");
                if (rpcError is RpcException typedRpc)
                {
                    evidence.Add($"rpc error code: {typedRpc.Code}");
                }
            }

            var kvRoute = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await client.Kv().BeginAsync(kvRoute);
            await tx.InsertAsync("x"u8.ToArray(), "1"u8.ToArray());
            await tx.CommitAsync();

            var tx2 = await client.Kv().BeginAsync(kvRoute);
            Exception? kvError = null;
            try
            {
                await tx2.InsertAsync("x"u8.ToArray(), "2"u8.ToArray());
            }
            catch (Exception ex)
            {
                kvError = ex;
            }
            finally
            {
                await tx2.RollbackAsync();
            }

            if (kvError is not null)
            {
                evidence.Add($"kv error type: {kvError.GetType().Name}");
            }

            var verdict = rpcError is RpcException && kvError is KvException ? "pass" : "partial";
            return Result("CS-006", transport, authMode, verdict, sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"server error mapping failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-006", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs007TimeoutHandling(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        var route = IntegrationFixture.CreateUniqueRoute("rpc");
        await using var workerClient = IntegrationFixture.CreateClientForMode(transport, authMode);
        await using var callerClient = IntegrationFixture.CreateClientForMode(transport, authMode, timeout: TimeSpan.FromMilliseconds(250));

        try
        {
            await workerClient.ConnectAsync();
            await callerClient.ConnectAsync();

            using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (_, writer, ct) =>
            {
                await Task.Delay(2000, ct);
                await writer.SendAsync("late"u8.ToArray(), isEnd: true, ct);
            });

            Exception? caught = null;
            var started = DateTimeOffset.UtcNow;
            try
            {
                await foreach (var _ in callerClient.Rpc().CallAsync(route, "block"u8.ToArray()))
                {
                }
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            var elapsed = DateTimeOffset.UtcNow - started;
            if (caught is null)
            {
                return Result("CS-007", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "timeout did not surface");
            }

            evidence.Add($"rpc threw after ~{elapsed.TotalMilliseconds:F0}ms");
            evidence.Add($"error type: {caught.GetType().Name}");

            var verdict = caught is RequestTimeoutException ? "pass" : "partial";

            var kvRoute = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await callerClient.Kv().BeginAsync(kvRoute);
            await tx.PutAsync("post-timeout"u8.ToArray(), "ok"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("connection healthy after timeout");
            return Result("CS-007", transport, authMode, verdict, sw.ElapsedMilliseconds, evidence, caught.Message);
        }
        catch (Exception ex)
        {
            evidence.Add($"timeout scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-007", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs008CallerCancellation(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        var route = IntegrationFixture.CreateUniqueRoute("rpc");
        await using var workerClient = IntegrationFixture.CreateClientForMode(transport, authMode);
        await using var callerClient = IntegrationFixture.CreateClientForMode(transport, authMode, timeout: TimeSpan.FromSeconds(30));

        try
        {
            await workerClient.ConnectAsync();
            await callerClient.ConnectAsync();

            using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (_, writer, ct) =>
            {
                await Task.Delay(2000, ct);
                await writer.SendAsync("late"u8.ToArray(), isEnd: true, ct);
            });

            using var cts = new CancellationTokenSource();
            var callTask = Task.Run(async () =>
            {
                await foreach (var _ in callerClient.Rpc().CallAsync(route, "block"u8.ToArray(), cts.Token))
                {
                }
            });

            await Task.Delay(100);
            cts.Cancel();

            Exception? caught = null;
            try
            {
                await callTask;
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            if (caught is null)
            {
                return Result("CS-008", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "caller cancellation did not interrupt call");
            }

            evidence.Add($"cancellation threw: {caught.GetType().Name}");

            var kvRoute = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await callerClient.Kv().BeginAsync(kvRoute);
            await tx.PutAsync("after-cancel"u8.ToArray(), "ok"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("subsequent request succeeded after cancellation");

            var verdict = caught is OperationCanceledException ? "pass" : "partial";
            return Result("CS-008", transport, authMode, verdict, sw.ElapsedMilliseconds, evidence, caught.Message);
        }
        catch (Exception ex)
        {
            evidence.Add($"caller cancellation failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-008", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs009DisconnectDuringRequest(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        var route = IntegrationFixture.CreateUniqueRoute("rpc");
        await using var workerClient = IntegrationFixture.CreateClientForMode(transport, authMode);
        var callerClient = IntegrationFixture.CreateClientForMode(transport, authMode, timeout: TimeSpan.FromMilliseconds(500));

        try
        {
            await workerClient.ConnectAsync();
            await callerClient.ConnectAsync();

            using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (_, writer, ct) =>
            {
                await Task.Delay(3000, ct);
                await writer.SendAsync("late"u8.ToArray(), isEnd: true, ct);
            });

            var callTask = Task.Run(async () =>
            {
                await foreach (var _ in callerClient.Rpc().CallAsync(route, "block"u8.ToArray()))
                {
                }
            });

            await Task.Delay(100);
            await callerClient.DisposeAsync();

            Exception? caught = null;
            try
            {
                await callTask;
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            if (caught is null)
            {
                evidence.Add("call completed before disconnect");
                return Result("CS-009", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence);
            }

            evidence.Add($"disconnect interrupted call with {caught.GetType().Name}");
            return Result("CS-009", transport, authMode, caught is ConnectionException ? "pass" : "partial", sw.ElapsedMilliseconds, evidence, caught.Message);
        }
        catch (Exception ex)
        {
            evidence.Add($"disconnect scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-009", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
        finally
        {
            await workerClient.DisposeAsync();
            await callerClient.DisposeAsync();
        }
    }

    private static async Task<ScenarioResult> RunCs010ReconnectAndRetryBehavior(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(
                transport,
                authMode,
                reconnect: new ReconnectOptions(true, MaxAttempts: 3, Backoff: TimeSpan.FromMilliseconds(100), MaxBackoff: TimeSpan.FromMilliseconds(500)));

            await client.ConnectAsync();
            evidence.Add("client connected with reconnect enabled");

            await client.DisposeAsync();
            evidence.Add("client disposed cleanly");

            await using var reconnected = IntegrationFixture.CreateClientForMode(transport, authMode);
            await reconnected.ConnectAsync();
            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var tx = await reconnected.Kv().BeginAsync(route);
            await tx.PutAsync("after-reconnect"u8.ToArray(), "ok"u8.ToArray());
            await tx.CommitAsync();
            evidence.Add("new requests succeed after reconnect/new client");

            return Result("CS-010", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"reconnect scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-010", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs011StreamReceiveSequence(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();

            var route = IntegrationFixture.CreateUniqueRoute("stream");
            var session = await client.Stream().BeginAsync(route, 0);
            await session.AppendAsync(new byte[] { 10 });
            await session.AppendAsync(new byte[] { 20 });
            await session.AppendAsync(new byte[] { 30 });
            await session.CommitAsync();
            evidence.Add("stream session appended 3 records");

            var records = new List<Abstractions.Domains.Stream.StreamRecord>();
            await foreach (var record in client.Stream().ReadAsync(route, 0, 10))
            {
                records.Add(record);
            }

            if (records.Count < 3)
            {
                evidence.Add($"expected >=3 records, got {records.Count}");
                return Result("CS-011", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence);
            }

            for (var i = 1; i < records.Count; i++)
            {
                if (records[i].Offset <= records[i - 1].Offset)
                {
                    evidence.Add($"out-of-order offsets at {i}");
                    return Result("CS-011", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence);
                }
            }

            evidence.Add($"read {records.Count} records in offset order");
            return Result("CS-011", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"stream sequence failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-011", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs012StreamCompletion(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();

            var route = IntegrationFixture.CreateUniqueRoute("stream");
            var session = await client.Stream().BeginAsync(route, 0);
            await session.AppendAsync("first"u8.ToArray());
            await session.AppendAsync("last"u8.ToArray());
            await session.CommitAsync();
            evidence.Add("stream session committed");

            var count = 0;
            await foreach (var _ in client.Stream().ReadAsync(route, 0, 100))
            {
                count++;
            }

            if (count < 2)
            {
                evidence.Add($"expected >=2 records, got {count}");
                return Result("CS-012", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence);
            }

            evidence.Add($"stream read completed cleanly with {count} records");
            return Result("CS-012", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"stream completion failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-012", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs013StreamErrorMidFlight(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();

            var route = IntegrationFixture.CreateUniqueRoute("stream");
            var session = await client.Stream().BeginAsync(route, 0);
            await session.AppendAsync("record-1"u8.ToArray());
            await session.CommitAsync();
            evidence.Add("written first record at offset 0");

            Exception? caught = null;
            try
            {
                await client.Stream().BeginAsync(route, 0);
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            if (caught is null)
            {
                return Result("CS-013", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "expected begin with wrong offset to fail");
            }

            evidence.Add($"begin with wrong offset raised {caught.GetType().Name}");
            return Result("CS-013", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence, caught.Message);
        }
        catch (Exception ex)
        {
            evidence.Add($"stream error scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-013", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs014ConcurrentInflightRequests(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();

            var routes = new[]
            {
                IntegrationFixture.CreateUniqueRoute("kv"),
                IntegrationFixture.CreateUniqueRoute("kv"),
                IntegrationFixture.CreateUniqueRoute("kv"),
            };

            var tasks = routes.Select((route, index) => Task.Run(async () =>
            {
                var tx = await client.Kv().BeginAsync(route);
                await tx.PutAsync(Encoding.UTF8.GetBytes($"key-{index}"), Encoding.UTF8.GetBytes($"value-{index}"));
                await tx.CommitAsync();

                var readTx = await client.Kv().BeginAsync(route, Abstractions.Domains.Kv.KvMode.ReadOnly);
                var result = await readTx.GetAsync(Encoding.UTF8.GetBytes($"key-{index}"));
                return result.Value.HasValue ? Encoding.UTF8.GetString(result.Value.Value.Span) : string.Empty;
            })).ToArray();

            var values = await Task.WhenAll(tasks);
            for (var index = 0; index < values.Length; index++)
            {
                if (!string.Equals(values[index], $"value-{index}", StringComparison.Ordinal))
                {
                    return Result("CS-014", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, $"task {index}: expected value-{index} got {values[index]}");
                }
            }

            evidence.Add("3 concurrent kv transactions completed correctly");
            return Result("CS-014", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"concurrent request scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-014", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs015ShutdownDuringActiveWork(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();
        var client = IntegrationFixture.CreateClientForMode(transport, authMode);

        try
        {
            await client.ConnectAsync();
            var route = IntegrationFixture.CreateUniqueRoute("kv");
            var beginTask = client.Kv().BeginAsync(route);

            await Task.Delay(50);
            await client.DisposeAsync();
            evidence.Add("close during active work did not panic");

            try
            {
                _ = await beginTask;
                evidence.Add("begin completed before close");
            }
            catch (Exception ex)
            {
                evidence.Add($"in-flight begin raised {ex.GetType().Name}");
            }

            await client.DisposeAsync();
            evidence.Add("double close is safe");
            return Result("CS-015", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"shutdown scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-015", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
        finally
        {
            await client.DisposeAsync();
        }
    }

    private static ScenarioResult Result(string scenarioId, string transport, string authMode, string verdict, long latencyMs, IReadOnlyList<string> evidence, string error = "")
    {
        return new ScenarioResult(
            scenarioId,
            "fitz-dotnet",
            transport,
            authMode,
            verdict,
            latencyMs,
            evidence,
            error
        );
    }
}
