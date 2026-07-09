using System.Text;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed partial class ConformanceSmokeTests
{
    [Fact]
    public async Task should_fail_invalid_jwt_auth_when_connecting()
    {
        var transport = IntegrationFixture.GetConformanceTransport();
        var result = await RunCs002AuthFailure(transport);
        Assert.Equal("pass", result.Verdict);
    }

    [Fact]
    public async Task should_write_json_result_given_enabled_flag_when_running_conformance_suite()
    {
        var config = IntegrationFixture.GetConformanceRunConfig();
        var aggregate = await RunConformanceSuiteAsync(config);

        Assert.Equal(17, aggregate.Scenarios.Count);
        Assert.Equal(config.ClientName, aggregate.Client);
        Assert.Equal(config.Transport, aggregate.Transport);
        Assert.Equal(config.AuthMode, aggregate.AuthMode);
        Assert.True(File.Exists(config.OutputPath));
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

            await using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (_, writer, ct) =>
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

            await using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (_, writer, ct) =>
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

            await using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (_, writer, ct) =>
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
        var disconnectObserved = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reconnectObserved = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var lifecycleEvents = new List<string>();
        var lifecycleGate = new object();
        var reconnect = new ReconnectOptions(true, MaxAttempts: 20, Backoff: TimeSpan.FromMilliseconds(100), MaxBackoff: TimeSpan.FromMilliseconds(500));
        var observability = new FitzObservabilityOptions(
            OnLifecycleEvent: evt =>
            {
                lock (lifecycleGate)
                {
                    lifecycleEvents.Add(evt.Event);
                }

                if (string.Equals(evt.Event, "connection_lost", StringComparison.Ordinal))
                {
                    disconnectObserved.TrySetResult(true);
                }

                if (string.Equals(evt.Event, "reconnect_succeeded", StringComparison.Ordinal))
                {
                    reconnectObserved.TrySetResult(true);
                }
            });
        await using var client = IntegrationFixture.CreateClientForMode(
            transport,
            authMode,
            timeout: TimeSpan.FromSeconds(5),
            reconnect: reconnect,
            observability: observability);
        await using var responderClient = IntegrationFixture.CreateClientForMode(
            transport,
            authMode,
            timeout: TimeSpan.FromSeconds(5),
            reconnect: new ReconnectOptions(false));
        var releasePendingCall = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        RpcWorkerRegistration? restoredWorker = null;
        RpcWorkerRegistration? responderWorker = null;
        Client? verifierClient = null;

        try
        {
            await client.ConnectAsync();
            await responderClient.ConnectAsync();
            evidence.Add("client connected with reconnect enabled");
            evidence.Add("responder client connected");

            var kvRoute = IntegrationFixture.CreateUniqueRoute("kv");
            var kvTransaction = await client.Kv().BeginAsync(kvRoute);

            var queueRoute = IntegrationFixture.CreateUniqueRoute("queue");
            await client.Queue().EnqueueAsync(queueRoute, "queued-before-reconnect"u8.ToArray());
            var reservedItems = await client.Queue().ReserveAsync(queueRoute, leaseSeconds: 30, batchSize: 1);
            var reservedItem = Assert.Single(reservedItems);

            var leaseRoute = IntegrationFixture.CreateUniqueRoute("lease");
            var lease = await client.Lease().AcquireAsync(leaseRoute, ttlSecs: 30);

            var streamRoute = IntegrationFixture.CreateUniqueRoute("stream");
            var streamSession = await client.Stream().BeginAsync(streamRoute);

            var restoredWorkerRoute = IntegrationFixture.CreateUniqueRoute("rpc");
            restoredWorker = await client.Rpc().RegisterWorkerAsync(restoredWorkerRoute, async (_, writer, ct) =>
            {
                await writer.SendAsync("same-client-worker"u8.ToArray(), isEnd: true, ct);
            });
            evidence.Add("client registered session-bound handles and an rpc worker");

            var pendingCallRoute = IntegrationFixture.CreateUniqueRoute("rpc");
            var pendingCallStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            responderWorker = await responderClient.Rpc().RegisterWorkerAsync(pendingCallRoute, async (_, writer, ct) =>
            {
                pendingCallStarted.TrySetResult(true);
                await releasePendingCall.Task.WaitAsync(ct).ConfigureAwait(false);
                await writer.SendAsync("late"u8.ToArray(), isEnd: true, ct);
            });

            var pendingCallTask = Task.Run(async () =>
            {
                await foreach (var _ in client.Rpc().CallAsync(pendingCallRoute, "block"u8.ToArray()))
                {
                }
            });

            await pendingCallStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            evidence.Add("started a pending rpc call before forcing disconnect");

            await IntegrationFixture.RestartBrokerForModeAsync(transport, authMode);
            evidence.Add("restarted the live broker service");

            await disconnectObserved.Task.WaitAsync(TimeSpan.FromSeconds(15));
            evidence.Add("client observed connection loss on the same instance");

            await client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
                Timeout: TimeSpan.FromSeconds(20),
                Backoff: TimeSpan.FromMilliseconds(100),
                MaxBackoff: TimeSpan.FromMilliseconds(500)));
            await reconnectObserved.Task.WaitAsync(TimeSpan.FromSeconds(20));
            evidence.Add("same client returned to authenticated state after reconnect");

            var rpcDisconnect = await CaptureExceptionAsync(() => pendingCallTask);
            if (rpcDisconnect is not ConnectionException and not RequestTimeoutException)
            {
                evidence.Add($"pending rpc call surfaced {rpcDisconnect?.GetType().Name ?? "no error"}");
                return Result("CS-010", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, rpcDisconnect?.Message ?? "pending rpc call unexpectedly succeeded");
            }

            evidence.Add($"pending rpc call terminated with {rpcDisconnect!.GetType().Name}");

            await AssertReconnectInvalidationAsync(
                () => kvTransaction.GetAsync("after-reconnect"u8.ToArray()),
                ex => ex is KvException kv && string.Equals(kv.Code, "TX_CLOSED", StringComparison.Ordinal),
                "kv transaction invalidated after disconnect",
                evidence);
            await AssertReconnectInvalidationAsync(
                () => reservedItem.CompleteAsync(),
                ex => ex is QueueException queue && string.Equals(queue.Code, "ITEM_CLOSED", StringComparison.Ordinal),
                "queue handle invalidated after disconnect",
                evidence);
            await AssertReconnectInvalidationAsync(
                () => lease.ExtendAsync(30),
                ex => ex is LeaseException leaseError && string.Equals(leaseError.Code, "CLOSED", StringComparison.Ordinal),
                "lease handle invalidated after disconnect",
                evidence);
            await AssertReconnectInvalidationAsync(
                () => streamSession.AppendAsync(0, "after-reconnect"u8.ToArray()),
                ex => ex is StreamException stream && string.Equals(stream.Code, "SESSION_CLOSED", StringComparison.Ordinal),
                "stream session invalidated after disconnect",
                evidence);

            await client.ConnectWhenReadyAsync(new ConnectWhenReadyOptions(
                Timeout: TimeSpan.FromSeconds(20),
                Backoff: TimeSpan.FromMilliseconds(100),
                MaxBackoff: TimeSpan.FromMilliseconds(500)));
            var postReconnectRoute = IntegrationFixture.CreateUniqueRoute("kv");
            var postReconnectTx = await client.Kv().BeginAsync(postReconnectRoute);
            await postReconnectTx.PutAsync("post-reconnect"u8.ToArray(), "ok"u8.ToArray());
            await postReconnectTx.CommitAsync();
            evidence.Add("same client completed a new kv transaction after reconnect");

            verifierClient = IntegrationFixture.CreateClientForMode(
                transport,
                authMode,
                timeout: TimeSpan.FromSeconds(5),
                reconnect: new ReconnectOptions(false));
            await verifierClient.ConnectAsync();

            var rpcResponses = new List<string>();
            await foreach (var frame in verifierClient.Rpc().CallAsync(restoredWorkerRoute, "verify"u8.ToArray()))
            {
                rpcResponses.Add(Encoding.UTF8.GetString(frame.Body.Span));
            }

            if (rpcResponses.Count != 1 || !string.Equals(rpcResponses[0], "same-client-worker", StringComparison.Ordinal))
            {
                evidence.Add($"restored worker returned {rpcResponses.Count} frames");
                return Result("CS-010", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "rpc worker was not restored on the reconnected client");
            }

            evidence.Add("same-client rpc worker was restored after reconnect");

            lock (lifecycleGate)
            {
                evidence.Add($"lifecycle events: {string.Join(", ", lifecycleEvents)}");
            }

            return Result("CS-010", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"reconnect scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-010", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
        finally
        {
            releasePendingCall.TrySetResult(true);
            await DisposeQuietlyAsync(verifierClient).ConfigureAwait(false);
            await DisposeQuietlyAsync(responderWorker).ConfigureAwait(false);
            await DisposeQuietlyAsync(restoredWorker).ConfigureAwait(false);
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
            var session = await client.Stream().BeginAsync(route);
            await session.AppendAsync(0, new byte[] { 10 });
            await session.AppendAsync(1, new byte[] { 20 });
            await session.AppendAsync(2, new byte[] { 30 });
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
            var session = await client.Stream().BeginAsync(route);
            await session.AppendAsync(0, "first"u8.ToArray());
            await session.AppendAsync(1, "last"u8.ToArray());
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
            var session = await client.Stream().BeginAsync(route);
            await session.AppendAsync(0, "record-1"u8.ToArray());
            await session.CommitAsync();
            evidence.Add("written first record at offset 0");

            Exception? caught = null;
            try
            {
                var wrongSession = await client.Stream().BeginAsync(route);
                await wrongSession.AppendAsync(0, "record-2"u8.ToArray());
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            if (caught is null)
            {
                return Result("CS-013", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "expected append with wrong offset to fail");
            }

            evidence.Add($"append with wrong offset raised {caught.GetType().Name}");
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

    private static async Task<ScenarioResult> RunCs017BoundedConcurrencyUnderBurstLoad(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(
                transport,
                authMode,
                timeout: TimeSpan.FromMilliseconds(1500),
                maxInFlightRequests: 16);
            await using var workerClient = IntegrationFixture.CreateClientForMode(
                transport,
                authMode,
                timeout: TimeSpan.FromSeconds(5),
                asyncHandlers: new AsyncHandlerOptions(MaxConcurrency: 1));

            await client.ConnectAsync();
            await workerClient.ConnectAsync();

            var route = IntegrationFixture.CreateUniqueRoute("rpc");
            await using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (req, writer, ct) =>
            {
                await Task.Delay(500, ct);
                await writer.SendAsync(req.Body, isEnd: true, ct);
            });

            await using var firstEnumerator = client.Rpc().CallAsync(route, "first"u8.ToArray()).GetAsyncEnumerator();
            await using var secondEnumerator = client.Rpc().CallAsync(route, "second"u8.ToArray()).GetAsyncEnumerator();

            var firstNext = firstEnumerator.MoveNextAsync().AsTask();
            var secondNext = secondEnumerator.MoveNextAsync().AsTask();
            var secondSettledEarly = await Task.WhenAny(secondNext, Task.Delay(100)) == secondNext;

            if (secondSettledEarly)
            {
                evidence.Add("second RPC call completed too early");
                return Result("CS-017", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "second call should stay queued behind the first");
            }

            evidence.Add("second RPC call remained pending while first was in flight");
            evidence.Add("configured maxInFlightRequests=16, worker maxConcurrency=1, and burst size=2");

            if (!await firstNext.ConfigureAwait(false) || !await secondNext.ConfigureAwait(false))
            {
                return Result("CS-017", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "expected both RPC calls to yield one response frame");
            }

            if (!string.Equals(Encoding.UTF8.GetString(firstEnumerator.Current.Body.Span), "first", StringComparison.Ordinal)
                || !string.Equals(Encoding.UTF8.GetString(secondEnumerator.Current.Body.Span), "second", StringComparison.Ordinal))
            {
                return Result("CS-017", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "burst RPC responses were not correlated to their request bodies");
            }

            if (await firstEnumerator.MoveNextAsync().ConfigureAwait(false) || await secondEnumerator.MoveNextAsync().ConfigureAwait(false))
            {
                return Result("CS-017", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "expected single-frame RPC responses");
            }

            evidence.Add("both burst RPC calls completed with correlated responses");

            return Result("CS-017", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"bounded concurrency scenario failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-017", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static async Task<ScenarioResult> RunCs016FilteredStreamReplay(string transport, string authMode)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var evidence = new List<string>();

        try
        {
            await using var client = IntegrationFixture.CreateClientForMode(transport, authMode);
            await client.ConnectAsync();

            var route = IntegrationFixture.CreateUniqueRoute("stream");
            var session = await client.Stream().BeginAsync(route);

            var firstOffset = await session.AppendAsync(0, "alpha"u8.ToArray(), discriminator: "proj.alpha");
            var secondOffset = await session.AppendAsync(1, "beta"u8.ToArray(), discriminator: "audit.beta");
            await session.CommitAsync();

            if (firstOffset is null || secondOffset is null)
            {
                evidence.Add("stream append did not return offsets");
                return Result("CS-016", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, "append returned null offset");
            }

            evidence.Add($"appended records at offsets {firstOffset.Value} and {secondOffset.Value}");

            var filter = new StreamFilterSet
            {
                Clauses = new[]
                {
                    new StreamFilterClause
                    {
                        Kind = StreamFilterClauseKind.Equals,
                        Value = "proj.alpha",
                    },
                },
            };

            var records = new List<StreamRecord>();
            await foreach (var record in client.Stream().ReadAsync(route, 0, 10, filter))
            {
                records.Add(record);
            }

            if (records.Count != 1)
            {
                evidence.Add($"compatibility read returned {records.Count} records");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, $"expected 1 filtered record, got {records.Count}");
            }

            if (records[0].Offset != firstOffset.Value)
            {
                evidence.Add($"compatibility read returned offset {records[0].Offset} instead of {firstOffset.Value}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "filtered read did not preserve the matching offset");
            }

            if (!string.Equals(Encoding.UTF8.GetString(records[0].Body), "alpha", StringComparison.Ordinal))
            {
                evidence.Add($"compatibility read returned {Encoding.UTF8.GetString(records[0].Body)}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "filtered read returned the wrong body");
            }

            var page = await client.Stream().ReadPageAsync(route, 0, 10, filter);
            if (page.Items.Count != 2)
            {
                evidence.Add($"page returned {page.Items.Count} items");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, $"expected 2 page items, got {page.Items.Count}");
            }

            var firstPageItem = page.Items[0];
            var firstRecord = firstPageItem.Record;
            if (firstPageItem.Kind != StreamReadItemKind.Event || firstRecord is null)
            {
                evidence.Add($"first page item kind was {firstPageItem.Kind}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "expected first page item to be an event");
            }

            if (firstRecord.Offset != firstOffset.Value)
            {
                evidence.Add($"first page item offset was {firstRecord.Offset}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "first page item offset mismatch");
            }

            if (!string.Equals(Encoding.UTF8.GetString(firstRecord.Body), "alpha", StringComparison.Ordinal))
            {
                evidence.Add($"first page item body was {Encoding.UTF8.GetString(firstRecord.Body)}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "first page item body mismatch");
            }

            if (page.Items[1].Kind != StreamReadItemKind.Filtered || page.Items[1].Reason != StreamFilteredReason.ServerFilter)
            {
                evidence.Add($"second page item kind was {page.Items[1].Kind} with reason {page.Items[1].Reason}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "expected synthetic filtered marker");
            }

            if (page.Items[1].Offset != secondOffset.Value)
            {
                evidence.Add($"filtered marker offset was {page.Items[1].Offset}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "filtered marker offset mismatch");
            }

            if (page.Cursor.LastResourceOffset != secondOffset.Value || page.Cursor.HasMore)
            {
                evidence.Add($"cursor last_resource_offset={page.Cursor.LastResourceOffset}, has_more={page.Cursor.HasMore}");
                return Result("CS-016", transport, authMode, "partial", sw.ElapsedMilliseconds, evidence, "cursor did not advance through filtered offsets");
            }

            evidence.Add("filtered replay returned the matching record plus synthetic filtered metadata in order");
            return Result("CS-016", transport, authMode, "pass", sw.ElapsedMilliseconds, evidence);
        }
        catch (Exception ex)
        {
            evidence.Add($"filtered stream replay failed: {ex.GetType().Name}: {ex.Message}");
            return Result("CS-016", transport, authMode, "fail", sw.ElapsedMilliseconds, evidence, ex.Message);
        }
    }

    private static ScenarioResult Result(string scenarioId, string transport, string authMode, string verdict, long latencyMs, IReadOnlyList<string> evidence, string notes = "", IReadOnlyDictionary<string, object?>? evidenceFields = null)
    {
        var metadata = ConformanceScenarioCatalog.Get(scenarioId);
        var renderedEvidence = evidenceFields is null
            ? evidence.ToArray()
            : evidence.Concat(evidenceFields.Select(entry => $"{entry.Key}={entry.Value}")).ToArray();
        return new ScenarioResult(
            scenarioId,
            metadata.Title,
            metadata.Priority,
            IntegrationFixture.GetConformanceClientName(),
            transport,
            authMode,
            verdict,
            latencyMs,
            renderedEvidence,
            string.IsNullOrWhiteSpace(notes) ? null : notes
        );
    }

    private static async Task AssertReconnectInvalidationAsync(Func<Task> operation, Func<Exception, bool> predicate, string successEvidence, ICollection<string> evidence)
    {
        var ex = await CaptureExceptionAsync(operation).ConfigureAwait(false);
        if (ex is null)
        {
            throw new InvalidOperationException($"{successEvidence} did not throw after reconnect.");
        }

        if (!predicate(ex))
        {
            throw new InvalidOperationException($"{successEvidence} surfaced {ex.GetType().Name}: {ex.Message}");
        }

        evidence.Add(successEvidence);
    }

    private static async Task<Exception?> CaptureExceptionAsync(Func<Task> operation)
    {
        try
        {
            await operation().ConfigureAwait(false);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static async Task DisposeQuietlyAsync(IAsyncDisposable? resource)
    {
        if (resource is null)
        {
            return;
        }

        try
        {
            await resource.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
        }
    }
}
