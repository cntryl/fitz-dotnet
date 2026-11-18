using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcTimingTests
{
    const string Route = "rpc://prod/app/work";
    static readonly TimeSpan ConfirmationWindow = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ShouldEndEnumerationGivenPendingCancellationSendWhenCleanupBudgetExpires()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var sendingCancel = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        using var rpc = new RpcClient(
            static (_, _, _) => ValueTask.FromResult(ReadOnlyMemory<byte>.Empty),
            async (messageType, _, ct) =>
            {
                if (messageType == MessageTypes.RpcCancel)
                {
                    sendingCancel.SetResult();
                    await releaseSend.Task.WaitAsync(ct);
                }
            },
            registerNotificationHandler: static (_, _) => new Registration(),
            timeProvider: time);
        var call = rpc.CallAsync(Route, ReadOnlyMemory<byte>.Empty, cancellation.Token);
        await using var enumerator = call.GetAsyncEnumerator();
        var response = enumerator.MoveNextAsync().AsTask();

        // Act
        await cancellation.CancelAsync();
        await sendingCancel.Task.WaitAsync(TimeSpan.FromSeconds(3));
        time.Advance(ConfirmationWindow);

        // Assert
        try
        {
            Assert.Equal(RpcCancellationOutcome.Unconfirmed, await call.Cancellation.WaitAsync(TimeSpan.FromSeconds(3)));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => response.WaitAsync(TimeSpan.FromSeconds(3)));
        }
        finally
        {
            releaseSend.TrySetResult();
            await Record.ExceptionAsync(() => response.WaitAsync(TimeSpan.FromSeconds(3)));
        }
    }

    [Fact]
    public async Task ShouldTimeOutCallAndReportUnconfirmedGivenNoBrokerAcknowledgementWhenClockAdvances()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var cancelSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = CreateClient(time, send: (messageType, _) =>
        {
            if (messageType == MessageTypes.RpcCancel)
                cancelSent.TrySetResult();
        });
        var budget = TimeSpan.FromSeconds(2);
        var call = rpc.CallAsync(Route, "work"u8.ToArray(), budget);
        var consumed = Record.ExceptionAsync(async () =>
        {
            await foreach (var _ in call)
            {
            }
        });

        // Act
        await time.WaitForTimerAsync(budget);
        time.Advance(budget);
        var error = await consumed.WaitAsync(TimeSpan.FromSeconds(5));
        await cancelSent.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await time.WaitForTimerAsync(ConfirmationWindow);
        time.Advance(ConfirmationWindow - TimeSpan.FromTicks(1));
        var settledEarly = call.Cancellation.IsCompleted;
        time.Advance(TimeSpan.FromTicks(1));
        var outcome = await call.Cancellation.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.False(settledEarly);
        Assert.Equal(RpcCancellationOutcome.Unconfirmed, outcome);
    }

    [Fact]
    public async Task ShouldReportFullBudgetAndCancelWorkerGivenCallerBudgetWhenClockAdvances()
    {
        // Arrange
        var time = new ManualTimeProvider();
        Action<ReadOnlyMemory<byte>>? deliver = null;
        using var rpc = CreateClient(time, onRequestHandler: handler => deliver = handler);
        var started = new TaskCompletionSource<TimeSpan?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var worker = await rpc.RegisterWorkerAsync(Route, async (request, _, ct) =>
        {
            started.TrySetResult(request.RemainingTime);
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                cancelled.TrySetResult();
            }
        });
        var budget = TimeSpan.FromSeconds(3);
        using var request = RpcWire.EncodeRequest(RpcWire.GuidToNetworkBytes(Guid.NewGuid()), Route, "work"u8, budget);

        // Act
        deliver!(request.WrittenMemory);
        var remaining = await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await time.WaitForTimerAsync(budget);
        var cancelledEarly = cancelled.Task.IsCompleted;
        time.Advance(budget);
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.Equal(budget, remaining);
        Assert.False(cancelledEarly);
    }

    static RpcClient CreateClient(
        TimeProvider time,
        Action<ushort, ReadOnlyMemory<byte>>? send = null,
        Action<Action<ReadOnlyMemory<byte>>>? onRequestHandler = null) => new(
        (_, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0, 0, 0, 0, 0 }),
        (messageType, payload, _) =>
        {
            send?.Invoke(messageType, payload);
            return ValueTask.CompletedTask;
        },
        registerNotificationHandler: (messageType, handler) =>
        {
            if (messageType == MessageTypes.RpcRequest)
                onRequestHandler?.Invoke(handler);
            return new Registration();
        },
        timeProvider: time);

    sealed class Registration : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
