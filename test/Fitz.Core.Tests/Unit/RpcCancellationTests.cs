using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcCancellationTests
{
    [Fact]
    public async Task ShouldPreserveLegacyCallerFramingGivenLocalTimeout()
    {
        // Arrange
        byte[]? payload = null;
        using var rpc = CreateClient(_ => { }, send: (kind, request, _) =>
        {
            if (kind == MessageTypes.RpcRequest)
                payload = request;
            return Task.CompletedTask;
        }, capabilityBits: 0);
        var call = rpc.CallAsync("rpc://prod/app/work", "work"u8.ToArray(), TimeSpan.FromMilliseconds(20));

        // Act
        var error = await Record.ExceptionAsync(async () =>
        {
            await foreach (var _ in call)
            { }
        });
        var outcome = await call.Cancellation;

        // Assert
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.Equal(RpcCancellationOutcome.Unsupported, outcome);
        Assert.Equal(16 + 4 + "rpc://prod/app/work".Length + 4 + 4, payload!.Length);
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
        var terminalSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCleanup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = CreateClient(handler => receive = handler, send: (kind, _, _) =>
        {
            if (kind == MessageTypes.RpcResponse)
                terminalSent.TrySetResult();
            if (kind == MessageTypes.RpcCancel)
                acknowledged.TrySetResult();
            return Task.CompletedTask;
        });
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/work", async (_, writer, _) =>
        {
            await writer.SendAsync(ReadOnlyMemory<byte>.Empty, isEnd: true, CancellationToken.None);
            await releaseCleanup.Task;
        });

        // Act
        receive!(RequestFrame(3000));
        await terminalSent.Task.WaitAsync(TimeSpan.FromSeconds(1));
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
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.True(cancelled.Task.IsCompletedSuccessfully);
    }

    static RpcClient CreateClient(Action<Action<byte[]>> receive,
        Func<ushort, byte[], CancellationToken, Task>? send = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        uint capabilityBits = ServerCapabilities.RpcCancellationBit) => new(
        (_, _, _) => Task.FromResult(new byte[] { 0, 0, 0, 0, 0 }),
        send: send ?? ((_, _, _) => Task.CompletedTask),
        registerNotificationHandler: (kind, handler) =>
        {
            if (kind == MessageTypes.RpcRequest)
                receive(handler);
            return new Registration();
        },
        dispatchAsyncHandler: dispatchAsyncHandler,
        getConnectionClosedToken: getConnectionClosedToken,
        getCapabilityBits: () => capabilityBits);

    static byte[] RequestFrame(uint budget)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteBytes(new byte[16]);
        writer.WriteString("rpc://prod/app/work");
        writer.WriteU32(0);
        writer.WriteU8(1);
        writer.WriteU8(1);
        writer.WriteU32(budget);
        return writer.Build();
    }

    sealed class Registration : IDisposable
    {
        public void Dispose() { }
    }
}
