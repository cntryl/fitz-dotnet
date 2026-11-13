using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcSessionAndSchedulingTests
{
    [Fact]
    public async Task ShouldUseActualSendingSessionGivenReconnectBeforeSendWhenCallingRpc()
    {
        // Arrange
        using var previous = new CancellationTokenSource();
        using var sendingSession = new CancellationTokenSource();
        Action<byte[]>? response = null;
        using var rpc = new RpcClient(
            (_, _, _) => ValueTask.FromResult(ReadOnlyMemory<byte>.Empty),
            (_, _, _) => ValueTask.CompletedTask,
            registerNotificationHandler: (kind, handler) =>
            {
                if (kind == MessageTypes.RpcResponse)
                    response = handler;
                return new TestRegistration();
            },
            getConnectionClosedToken: () => previous.Token,
            sendInSession: async (_, payload, _) =>
            {
                // Sending may wait for reconnect before it chooses a transport. Its returned
                // lifetime, rather than the previously published token, owns this request.
                await previous.CancelAsync();
                var request = new BinaryBufferReader(payload);
                using var frame = new BinaryBufferWriter();
                frame.WriteBytes(request.ReadBytes(16));
                frame.WriteU64(0);
                frame.WriteU8(1);
                frame.WriteU32(3);
                frame.WriteBytes("new"u8);
                response!(frame.Build());
                return sendingSession.Token;
            });
        var call = rpc.CallAsync("rpc://prod/app/work", ReadOnlyMemory<byte>.Empty);
        var frames = new List<RpcResponseFrame>();

        // Act
        await foreach (var frame in call)
            frames.Add(frame);

        // Assert
        Assert.Equal("new"u8.ToArray(), Assert.Single(frames).Body.ToArray());
        Assert.Equal(RpcCancellationOutcome.NotRequested, await call.Cancellation);
    }

    [Fact]
    public async Task ShouldFailLostCallGivenSessionReplacedDuringSendWhenCallingRpc()
    {
        // Arrange
        using var oldSession = new CancellationTokenSource();
        using var replacement = new CancellationTokenSource();
        var currentSession = oldSession.Token;
        using var rpc = new RpcClient(
            (_, _, _) => Task.FromResult(Array.Empty<byte>()),
            send: async (type, _, _) =>
            {
                if (type == MessageTypes.RpcRequest)
                {
                    await oldSession.CancelAsync();
                    currentSession = replacement.Token;
                }
            },
            registerNotificationHandler: (_, _) => new TestRegistration(),
            getConnectionClosedToken: () => currentSession,
            connectionTimeout: TimeSpan.FromMilliseconds(100));

        // Act
        var call = rpc.CallAsync("rpc://prod/app/work", ReadOnlyMemory<byte>.Empty);
        await using var reader = call.GetAsyncEnumerator();

        // Assert
        await Assert.ThrowsAsync<ConnectionException>(() => reader.MoveNextAsync().AsTask());
        Assert.Equal(RpcCancellationOutcome.ConnectionClosed, await call.Cancellation);
    }

    [Fact]
    public async Task ShouldUseAvailableMatchingWorkerGivenBusyExactWorkerWhenDispatching()
    {
        // Arrange
        var harness = new WorkerHarness();
        using var rpc = harness.Client;
        var exactStarted = Signal();
        var wildcardStarted = Signal();
        var releaseExact = Signal();
        await using var exact = await rpc.RegisterWorkerAsync("rpc://prod/app/work", async (_, _, _) =>
        {
            exactStarted.TrySetResult();
            await releaseExact.Task;
        });
        await using var wildcard = await rpc.RegisterWorkerAsync("rpc://prod/app/*", (_, _, _) =>
        {
            wildcardStarted.TrySetResult();
            return ValueTask.CompletedTask;
        });

        // Act
        harness.Receive(Request(1, "rpc://prod/app/work"));
        await exactStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        harness.Receive(Request(2, "rpc://prod/app/work"));

        // Assert
        try
        {
            await wildcardStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
            Assert.Equal(0, harness.Rejections);
        }
        finally
        {
            releaseExact.TrySetResult();
        }
    }

    [Fact]
    public async Task ShouldWaitForMatchingCapacityGivenOutOfOrderCreditReleaseWhenDispatchingAnotherRoute()
    {
        // Arrange
        var harness = new WorkerHarness();
        using var rpc = harness.Client;
        var exactStarted = Signal();
        var wildcardStarted = Signal();
        var thirdStarted = Signal();
        var releaseExact = Signal();
        var releaseWildcard = Signal();
        await using var exact = await rpc.RegisterWorkerAsync("rpc://prod/app/work", async (_, _, _) =>
        {
            exactStarted.TrySetResult();
            await releaseExact.Task;
        });
        await using var wildcard = await rpc.RegisterWorkerAsync("rpc://prod/app/*", async (request, _, _) =>
        {
            if (request.Body.Span[0] == 2)
            {
                wildcardStarted.TrySetResult();
                await releaseWildcard.Task;
            }
            else
            {
                thirdStarted.TrySetResult();
            }
        });
        harness.Receive(Request(1, "rpc://prod/app/work"));
        await exactStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        harness.Receive(Request(2, "rpc://prod/app/work"));

        // Act
        try
        {
            await wildcardStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
            // The broker may have charged invocation 1 to the wildcard registration and
            // invocation 2 to the exact registration. Releasing 1 then admits another route,
            // although its only matching local gate is still occupied by invocation 2.
            releaseExact.TrySetResult();
            harness.Receive(Request(3, "rpc://prod/app/other"));
            Assert.False(thirdStarted.Task.IsCompleted);
            Assert.Equal(0, harness.Rejections);
            releaseWildcard.TrySetResult();

            // Assert
            await thirdStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
            Assert.Equal(0, harness.Rejections);
        }
        finally
        {
            releaseExact.TrySetResult();
            releaseWildcard.TrySetResult();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShouldCleanUpWaitingInvocationGivenCancellationOrBudgetWhenMatchingWorkerBusy(bool budgetExpires)
    {
        // Arrange
        var harness = new WorkerHarness();
        using var rpc = harness.Client;
        var firstStarted = Signal();
        var releaseFirst = Signal();
        var queuedCalls = 0;
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/*", async (request, _, _) =>
        {
            if (request.Body.Span[0] == 1)
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task;
            }
            else
            {
                Interlocked.Increment(ref queuedCalls);
            }
        });
        harness.Receive(Request(1, "rpc://prod/app/work"));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Act
        harness.Receive(Request(2, "rpc://prod/app/other", budgetExpires ? 30U : null));
        if (!budgetExpires)
            harness.Cancel(2);

        // Assert
        try
        {
            await harness.SecondCleanup.Task.WaitAsync(TimeSpan.FromSeconds(1));
            Assert.Equal(0, harness.Rejections);
            Assert.Equal(0, Volatile.Read(ref queuedCalls));
            releaseFirst.TrySetResult();
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    [Fact]
    public async Task ShouldCountCapacityWaitGivenLimitedBudgetWhenDispatching()
    {
        // Arrange
        var harness = new WorkerHarness();
        using var rpc = harness.Client;
        var firstStarted = Signal();
        var releaseFirst = Signal();
        var remainingTime = new TaskCompletionSource<TimeSpan?>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/*", async (request, _, _) =>
        {
            if (request.Body.Span[0] == 1)
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task;
            }
            else
            {
                remainingTime.TrySetResult(request.RemainingTime);
            }
        });
        harness.Receive(Request(1, "rpc://prod/app/work"));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Act
        harness.Receive(Request(2, "rpc://prod/app/other", 2000));
        await Task.Delay(100);
        releaseFirst.TrySetResult();

        // Assert
        var observed = await remainingTime.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.NotNull(observed);
        Assert.InRange(observed.Value, TimeSpan.Zero, TimeSpan.FromMilliseconds(1950));
        Assert.Equal(0, harness.Rejections);
    }

    [Fact]
    public async Task ShouldRejectBeyondBoundGivenWorkerAndWaitQueueFullWhenDispatching()
    {
        // Arrange
        var harness = new WorkerHarness();
        using var rpc = harness.Client;
        var firstStarted = Signal();
        var releaseFirst = Signal();
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/*", async (_, _, _) =>
        {
            firstStarted.TrySetResult();
            await releaseFirst.Task;
        });
        harness.Receive(Request(1, "rpc://prod/app/work"));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Act
        harness.Receive(Request(2, "rpc://prod/app/work"));
        harness.Receive(Request(3, "rpc://prod/app/work"));

        // Assert
        try
        {
            Assert.Equal(1, harness.Rejections);
            harness.Cancel(2);
            await harness.SecondCleanup.Task.WaitAsync(TimeSpan.FromSeconds(1));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    static byte[] Request(byte id, string route, uint? budget = null)
    {
        using var writer = new BinaryBufferWriter();
        var correlation = new byte[16];
        correlation[0] = id;
        writer.WriteBytes(correlation);
        writer.WriteString(route);
        writer.WriteU32(1);
        writer.WriteU8(id);
        writer.WriteU8(budget.HasValue ? (byte)1 : (byte)0);
        if (budget is { } remaining)
            writer.WriteU32(remaining);
        return writer.Build();
    }

    sealed class WorkerHarness
    {
        Action<byte[]>? _request;
        Action<byte[]>? _lifecycle;
        int _rejections;
        internal RpcClient Client { get; }
        internal TaskCompletionSource SecondCleanup { get; } = Signal();
        internal int Rejections => Volatile.Read(ref _rejections);

        internal WorkerHarness()
        {
            Client = new RpcClient(
                (_, _, _) => Task.FromResult(new byte[] { 0, 0, 0, 0, 0 }),
                send: (kind, payload, _) =>
                {
                    if (kind == MessageTypes.RpcResponse)
                        Interlocked.Increment(ref _rejections);
                    if (kind == MessageTypes.RpcCancel && payload[0] == 3 && payload[1] == 2)
                        SecondCleanup.TrySetResult();
                    return Task.CompletedTask;
                },
                registerNotificationHandler: (kind, handler) =>
                {
                    if (kind == MessageTypes.RpcRequest)
                        _request = handler;
                    if (kind == MessageTypes.RpcLifecycle)
                        _lifecycle = handler;
                    return new TestRegistration();
                });
        }

        internal void Receive(byte[] payload) => _request!(payload);

        internal void Cancel(byte id)
        {
            var payload = new byte[18];
            payload[0] = 2;
            payload[1] = id;
            payload[17] = 1;
            _lifecycle!(payload);
        }
    }
}
