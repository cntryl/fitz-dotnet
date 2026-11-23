using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcCancellationTests
{
    [Fact]
    public async Task ShouldReportUnconfirmedGivenShutdownDuringUncertainWrite()
    {
        // Arrange
        var writing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = CreateClient(_ => { }, send: async (_, _, _) =>
        {
            writing.SetResult();
            await release.Task;
            throw new IOException("transport lost during write");
        });
        var call = rpc.CallStreamingAsync("rpc://prod/app/work", ReadOnlyMemory<byte>.Empty);
        await using var reader = call.GetAsyncEnumerator();
        var pending = reader.MoveNextAsync().AsTask();
        await writing.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Act
        rpc.Dispose();
        release.SetResult();

        // Assert
        await Assert.ThrowsAsync<IOException>(() => pending);
        Assert.Equal(RpcCancellationOutcome.Unconfirmed, await call.Cancellation);
    }

    [Fact]
    public async Task ShouldCancelOnOriginatingSessionGivenEarlyExitAfterDispatch()
    {
        // Arrange
        Action<ReadOnlyMemory<byte>>? response = null;
        Action<ReadOnlyMemory<byte>>? lifecycle = null;
        using var owner = new CancellationTokenSource();
        using var replacement = new CancellationTokenSource();
        var current = owner.Token;
        var requests = 0;
        var cancellations = 0;
        using var rpc = new RpcClient(
            (_, _, _) => throw new InvalidOperationException(),
            (_, payload, _) =>
            {
                requests++;
                using var frame = new BinaryBufferWriter();
                frame.WriteBytes(payload.Span[..16]);
                frame.WriteU64(0);
                frame.WriteU8(0);
                frame.WriteLengthPrefixed("partial"u8);
                response!(frame.Build());
                return ValueTask.CompletedTask;
            },
            registerNotificationHandler: (type, handler) =>
            {
                if (type == MessageTypes.RpcResponse)
                    response = handler;


                if (type == MessageTypes.RpcLifecycle)
                    lifecycle = handler;

                return new TestRegistration();
            },
            getConnectionClosedToken: () => current,
            sendOnSession: (type, payload, session, _) =>
            {
                cancellations++;
                Assert.Equal(owner.Token, session);
                Assert.Equal(MessageTypes.RpcCancel, type);
                var result = payload.ToArray();
                result[0] = 4;
                result[17] = 2;
                lifecycle!(result);
                return ValueTask.CompletedTask;
            });
        var call = rpc.CallStreamingAsync("rpc://prod/app/work", ReadOnlyMemory<byte>.Empty);

        // Act
        await foreach (var frame in call)
        {
            Assert.Equal("partial"u8.ToArray(), frame.Body.ToArray());
            current = replacement.Token;
            break;
        }

        // Assert
        Assert.Equal(1, requests);
        Assert.Equal(1, cancellations);
        Assert.Equal(RpcCancellationOutcome.Forwarded, await call.Cancellation);
        Assert.Throws<InvalidOperationException>(() => call.GetAsyncEnumerator());
    }

    [Fact]
    public async Task ShouldReportUnsentGivenEnumeratorDisposedBeforeFirstMove()
    {
        // Arrange
        var sends = 0;
        using var rpc = CreateClient(_ => { }, send: (_, _, _) => { sends++; return Task.CompletedTask; });
        var call = rpc.CallStreamingAsync("rpc://prod/app/work", ReadOnlyMemory<byte>.Empty);

        // Act
        var enumerator = call.GetAsyncEnumerator();
        await enumerator.DisposeAsync();

        // Assert
        Assert.Equal(RpcCancellationOutcome.RequestNotSent, await call.Cancellation.WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.Throws<InvalidOperationException>(() => call.GetAsyncEnumerator());
        Assert.Equal(0, sends);
    }

    [Fact]
    public async Task ShouldDispatchOnceGivenConcurrentEnumerationsAndUncertainWrite()
    {
        // Arrange
        var sends = 0;
        using var rpc = CreateClient(_ => { }, send: (_, _, _) =>
        {
            Interlocked.Increment(ref sends);
            throw new IOException("write outcome unknown");
        });
        var call = rpc.CallStreamingAsync("rpc://prod/app/work", ReadOnlyMemory<byte>.Empty);

        // Act
        var attempts = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            await Record.ExceptionAsync(async () =>
            {
                await foreach (var frame in call)
                { }
            }))));

        // Assert
        Assert.Equal(7, attempts.Count(error => error is InvalidOperationException));
        Assert.Single(attempts, error => error is IOException);
        Assert.Equal(1, sends);
        Assert.Equal(RpcCancellationOutcome.Unconfirmed, await call.Cancellation);
    }

    [Fact]
    public async Task ShouldCleanUpBufferedInvocationGivenCancellationBeforeDeferredDispatch()
    {
        // Arrange
        Action<byte[]>? receive = null;
        Action<byte[]>? lifecycle = null;
        Func<CancellationToken, ValueTask>? dispatch = null;
        var invocations = 0;
        var releaseUnrelated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var unrelatedCancelled = false;
        var acknowledged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = CreateClient(handler => receive = handler,
            dispatchAsyncHandler: handler => { dispatch = handler; return true; },
            lifecycle: handler => lifecycle = handler,








            send: (kind, payload, _) =>
            {
                if (kind == MessageTypes.RpcCancel && payload[0] == 3 && payload[1] == 1)
                {
                    acknowledged.TrySetResult();
                }

                return Task.CompletedTask;
            });
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/work", async (_, _, ct) =>
        {
            invocations++;
            await releaseUnrelated.Task;
            unrelatedCancelled = ct.IsCancellationRequested;
        });
        var cancellation = new byte[18];
        cancellation[0] = 2;
        cancellation[1] = 1;
        cancellation[17] = 1;

        // Act
        receive!(RequestFrame(3000));
        var activeDispatch = dispatch!(CancellationToken.None).AsTask();
        receive!(RequestFrame(3000, correlationByte: 1));
        lifecycle!(cancellation);
        await acknowledged.Task.WaitAsync(TimeSpan.FromMilliseconds(300));
        await dispatch!(CancellationToken.None);
        releaseUnrelated.SetResult();
        await activeDispatch;

        // Assert
        Assert.Equal(1, invocations);
        Assert.False(unrelatedCancelled);
    }

    [Fact]
    public async Task ShouldIncludeBudgetWithoutOptionalCapabilitiesGivenLocalTimeout()
    {
        // Arrange
        byte[]? payload = null;
        using var rpc = CreateClient(_ => { }, send: (kind, request, _) =>
        {
            if (kind == MessageTypes.RpcRequest)
                payload = request;

            return Task.CompletedTask;
        });
        var call = rpc.CallStreamingAsync("rpc://prod/app/work", "work"u8.ToArray(), TimeSpan.FromMilliseconds(20));

        // Act
        var error = await Record.ExceptionAsync(async () =>
        {
            await foreach (var _ in call)
            { }
        });

        // Assert
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.Equal(16 + 4 + "rpc://prod/app/work".Length + 4 + 4 + 5, payload!.Length);
    }

    [Fact]
    public async Task ShouldDiscardQueuedInvocationGivenConnectionReplacement()
    {
        // Arrange
        Action<byte[]>? receive = null;
        Func<CancellationToken, ValueTask>? dispatch = null;
        using var oldConnection = new CancellationTokenSource();
        using var newConnection = new CancellationTokenSource();
        var connectionToken = oldConnection.Token;
        var invocations = 0;
        using var rpc = CreateClient(handler => receive = handler,
            dispatchAsyncHandler: handler => { dispatch = handler; return true; },
            getConnectionClosedToken: () => connectionToken);
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/work", (_, _, _) =>
        {
            invocations++;
            return ValueTask.CompletedTask;
        });

        // Act
        receive!(RequestFrame(3000));
        await oldConnection.CancelAsync();
        connectionToken = newConnection.Token;
        await dispatch!(CancellationToken.None);

        // Assert
        Assert.Equal(0, invocations);
    }

    [Fact]
    public async Task ShouldCountDispatchDelayAgainstInboundBudgetGivenDeferredHandler()
    {
        // Arrange
        Action<byte[]>? receive = null;
        Func<CancellationToken, ValueTask>? dispatch = null;
        var started = new TaskCompletionSource<(TimeSpan? Remaining, bool Cancelled)>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = CreateClient(handler => receive = handler,
            dispatchAsyncHandler: handler => { dispatch = handler; return true; });
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/work", (request, _, ct) =>
        {
            started.SetResult((request.RemainingTime, ct.IsCancellationRequested));
            return ValueTask.CompletedTask;
        });

        // Act
        receive!(RequestFrame(20));
        await Task.Delay(80);
        await dispatch!(CancellationToken.None);
        var observed = await started.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal(TimeSpan.Zero, observed.Remaining);
        Assert.True(observed.Cancelled);
    }

    [Fact]
    public async Task ShouldAcknowledgeOnlyAfterHandlerCleanupGivenEarlyTerminalResponse()
    {
        // Arrange
        Action<byte[]>? receive = null;
        Action<byte[]>? lifecycle = null;
        var terminalSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCleanup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = CreateClient(handler => receive = handler, lifecycle: handler => lifecycle = handler, send: (kind, _, _) =>
        {
            if (kind == MessageTypes.RpcResponse)
                terminalSent.TrySetResult();


            if (kind == MessageTypes.RpcCancel)
                acknowledged.TrySetResult();

            return Task.CompletedTask;
        });
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/work", async (_, writer, ct) =>
        {
            await writer.SendAsync(ReadOnlyMemory<byte>.Empty, isEnd: true, CancellationToken.None);
            using var registration = ct.Register(() => canceled.TrySetResult());
            await releaseCleanup.Task;
        });

        // Act
        receive!(RequestFrame(3000));
        await terminalSent.Task.WaitAsync(TimeSpan.FromSeconds(1));
        var cancellation = new byte[18];
        cancellation[0] = 2;
        cancellation[17] = 1;
        lifecycle!(cancellation);
        await canceled.Task.WaitAsync(TimeSpan.FromSeconds(1));
        var prematureAcknowledgement = acknowledged.Task.IsCompleted;
        releaseCleanup.SetResult();
        await acknowledged.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.False(prematureAcknowledgement);
    }

    [Fact]
    public async Task ShouldExpireWorkerTokenLocallyGivenNoBrokerCancellationSignal()
    {
        // Arrange
        Action<byte[]>? receive = null;
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = CreateClient(handler => receive = handler);
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/work", async (_, _, ct) =>
        {
            try
            { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
            finally { if (ct.IsCancellationRequested) cancelled.SetResult(); }
        });

        // Act
        receive!(RequestFrame(30));
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Assert
        Assert.True(cancelled.Task.IsCompletedSuccessfully);
    }

    static RpcClient CreateClient(
        Action<Action<byte[]>> receive,
        Func<ushort, byte[], CancellationToken, Task>? send = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        Action<Action<byte[]>>? lifecycle = null
    ) => new(
        (_, _, _) => Task.FromResult(new byte[] { 0, 0, 0, 0, 0 }),
        send: send ?? ((_, _, _) => Task.CompletedTask),
        registerNotificationHandler: (kind, handler) =>
        {
            if (kind == MessageTypes.RpcRequest)
                receive(handler);


            if (kind == MessageTypes.RpcLifecycle)
                lifecycle?.Invoke(handler);

            return new Registration();
        },
        dispatchAsyncHandler: dispatchAsyncHandler,
        getConnectionClosedToken: getConnectionClosedToken);

    static byte[] RequestFrame(uint budget, byte correlationByte = 0)
    {
        using var writer = new BinaryBufferWriter();
        var correlation = new byte[16];
        correlation[0] = correlationByte;
        writer.WriteBytes(correlation);
        writer.WriteString("rpc://prod/app/work");
        writer.WriteU32(0);
        writer.WriteU8(1);
        writer.WriteU32(budget);
        return writer.Build();
    }

    sealed class Registration : IDisposable
    {
        public void Dispose() { }
    }
}
