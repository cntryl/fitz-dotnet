namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class RpcCancellationChainTests
{
    [Fact]
    public async Task ShouldReceiveWorkerResponsesGivenOpaqueDispatchIdentity()
    {
        // Arrange
        var address = Environment.GetEnvironmentVariable("FITZ_RPC_CHAIN_ADDR")
            ?? IntegrationFixture.GetAnonymousWebSocketUrl();
        await using var caller = CreateClient(address);
        await using var worker = CreateClient(address);
        await caller.ConnectAsync();
        await worker.ConnectAsync();
        var route = $"rpc://chain/app/unary-{Guid.NewGuid():N}";
        await using var registration = await worker.Rpc.RegisterWorkerAsync(route, async (_, writer, ct) =>
        {
            await writer.SendAsync("first"u8.ToArray(), ct: ct);
            await writer.SendAsync("last"u8.ToArray(), isEnd: true, ct);
        });

        // Act
        var frames = new List<RpcResponseFrame>();
        await foreach (var frame in caller.Rpc.CallAsync(route, "request"u8.ToArray()))
            frames.Add(frame);

        // Assert
        Assert.Equal(2, frames.Count);
        Assert.Equal("first"u8.ToArray(), frames[0].Body.ToArray());
        Assert.Equal("last"u8.ToArray(), frames[1].Body.ToArray());
        Assert.Equal(0ul, frames[0].Sequence);
        Assert.Equal(1ul, frames[1].Sequence);
    }

    static Client CreateClient(string address)
    {
        var transport = address.StartsWith("tcp://", StringComparison.Ordinal) ? "tcp" : "websocket";
        return Environment.GetEnvironmentVariable("FITZ_BROKER_JWT_HMAC_SECRET") is null
            ? IntegrationFixture.CreateAnonymousClient(address, transport)
            : IntegrationFixture.CreateValidJwtClient(address, transport);
    }

    [Fact]
    public async Task ShouldCancelRealSdkCallChainGivenCallerCancellation()
    {
        // Arrange
        await using var chain = await Chain.StartAsync(TimeSpan.FromSeconds(3));

        // Act
        await chain.CancelAsync();
        await chain.WaitForCleanupAsync();

        // Assert
        Assert.IsAssignableFrom<OperationCanceledException>(await chain.Caller);
        Assert.Equal(RpcCancellationOutcome.Forwarded, await chain.Call.Cancellation);
    }

    [Fact]
    public async Task ShouldExpireRealSdkCallChainGivenInheritedDeadline()
    {
        // Arrange
        await using var chain = await Chain.StartAsync(TimeSpan.FromMilliseconds(700));

        // Act
        var error = await chain.Caller.WaitAsync(TimeSpan.FromSeconds(2));
        await chain.WaitForCleanupAsync();

        // Assert
        Assert.True(error is OperationCanceledException || error is RpcException { DomainCode: 6001 });
        Assert.InRange(chain.Elapsed.TotalMilliseconds, 0, 1500);
    }

    sealed class Chain : IAsyncDisposable
    {
        readonly Client[] _clients;
        readonly RpcWorkerRegistration[] _workers;
        readonly CancellationTokenSource _callerCancellation;
        readonly Task[] _cleanup;
        readonly long _startedAt;

        internal RpcCall Call { get; }
        internal Task<Exception?> Caller { get; }
        internal TimeSpan Elapsed => System.Diagnostics.Stopwatch.GetElapsedTime(_startedAt);

        Chain(Client[] clients, RpcWorkerRegistration[] workers, CancellationTokenSource cancellation,
            Task[] cleanup, RpcCall call, Task<Exception?> caller, long startedAt)
        {
            _clients = clients;
            _workers = workers;
            _callerCancellation = cancellation;
            _cleanup = cleanup;
            Call = call;
            Caller = caller;
            _startedAt = startedAt;
        }

        internal static async Task<Chain> StartAsync(TimeSpan budget)
        {
            var address = Environment.GetEnvironmentVariable("FITZ_RPC_CHAIN_ADDR")
                ?? IntegrationFixture.GetAnonymousWebSocketUrl();
            var clients = Enumerable.Range(0, 3)
                .Select(_ => CreateClient(address)).ToArray();
            foreach (var client in clients)
            {
                await client.ConnectAsync();
            }
            var suffix = Guid.NewGuid().ToString("N");
            var routeB = $"rpc://chain/app/b-{suffix}";
            var routeC = $"rpc://chain/app/c-{suffix}";
            var childReceived = new TaskCompletionSource<TimeSpan>(TaskCreationOptions.RunContinuationsAsynchronously);
            var bCleaned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var cCleaned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var workerC = await clients[2].Rpc.RegisterWorkerAsync(routeC, async (request, _, ct) =>
            {
                childReceived.TrySetResult(request.RemainingTime!.Value);
                try
                { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
                finally { cCleaned.TrySetResult(); }
            });
            var workerB = await clients[1].Rpc.RegisterWorkerAsync(routeB, async (request, responseWriter, ct) =>
            {
                try
                {
                    await Task.Delay(25, ct);
                    await foreach (var _ in clients[1].Rpc.CallAsync(routeC, "child"u8.ToArray(), request.RemainingTime, ct))
                    { }
                }
                finally { bCleaned.TrySetResult(); }
            });
            var cancellation = new CancellationTokenSource();
            var started = System.Diagnostics.Stopwatch.GetTimestamp();
            var call = clients[0].Rpc.CallAsync(routeB, "parent"u8.ToArray(), budget, cancellation.Token);
            var caller = DrainAsync(call);
            var remaining = await childReceived.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.True(remaining < budget, "Downstream budget must include parent processing time.");
            return new Chain(clients, [workerB, workerC], cancellation,
                [bCleaned.Task, cCleaned.Task], call, caller, started);
        }

        internal async Task CancelAsync() => await _callerCancellation.CancelAsync();

        internal async Task WaitForCleanupAsync() =>
            await Task.WhenAll(_cleanup).WaitAsync(TimeSpan.FromSeconds(3));

        public async ValueTask DisposeAsync()
        {
            await _callerCancellation.CancelAsync();
            foreach (var worker in _workers)
                await worker.DisposeAsync();
            foreach (var client in _clients)
                await client.DisposeAsync();
            _callerCancellation.Dispose();
        }

        static async Task<Exception?> DrainAsync(RpcCall call) => await Record.ExceptionAsync(async () =>
        {
            await foreach (var _ in call)
            { }
        });
    }
}
