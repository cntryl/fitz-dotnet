using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class CleanupBudgetTimingTests
{
    static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ShouldAbandonRollbackGivenStalledBrokerWhenTransactionCleanupBudgetElapses()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var broker = new StalledBroker();
        var transaction = new KvTransaction(broker.RequestAsync, "kv://prod/app/data", 7, timeProvider: time);

        // Act
        var disposal = transaction.DisposeAsync().AsTask();
        await broker.Started.WaitAsync(TimeSpan.FromSeconds(5));
        await time.WaitForTimerAsync(CleanupBudget);
        var completedEarly = disposal.IsCompleted;
        time.Advance(CleanupBudget);
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.False(completedEarly);
        Assert.True(broker.Cancelled);
    }

    [Fact]
    public async Task ShouldAbandonRollbackGivenStalledBrokerWhenStreamSessionCleanupBudgetElapses()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var broker = new StalledBroker();
        var session = new StreamSession(broker.RequestAsync, 7, timeProvider: time);

        // Act
        var disposal = session.DisposeAsync().AsTask();
        await broker.Started.WaitAsync(TimeSpan.FromSeconds(5));
        await time.WaitForTimerAsync(CleanupBudget);
        var completedEarly = disposal.IsCompleted;
        time.Advance(CleanupBudget);
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.False(completedEarly);
        Assert.True(broker.Cancelled);
    }

    [Fact]
    public async Task ShouldFinishWorkerDisposalGivenStalledBrokerWhenClientClockReachesCleanupDeadline()
    {
        // Arrange
        var time = new ManualTimeProvider();
        await using var transport = new TestQueuedTransport();
        var config = new ClientConfig(transport.Url) { TransportFactory = _ => transport };
        await using var client = new FitzClient(config, time);
        await client.ConnectAsync();
        transport.QueueResponse(MessageTypes.RpcSubscribeWorker, [0, 0, 0, 0, 0]);
        var registration = await client.Rpc.RegisterWorkerAsync(
            "rpc://prod/app/work", static (_, _, _) => ValueTask.CompletedTask);
        var unregisterSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.AfterSend = _ => unregisterSent.TrySetResult();

        // Act
        var disposal = registration.DisposeAsync().AsTask();
        await unregisterSent.Task.WaitAsync(TimeSpan.FromSeconds(5));
        time.Advance(CleanupBudget - TimeSpan.FromTicks(1));
        var completedEarly = disposal.IsCompleted;
        time.Advance(TimeSpan.FromTicks(1));
        var error = await Record.ExceptionAsync(() => disposal.WaitAsync(TimeSpan.FromSeconds(2)));

        // Assert
        Assert.False(completedEarly);
        Assert.Null(error);
        Assert.True(client.IsConnected);
    }

    // Accepts the cleanup request and never answers, so only the cleanup budget ends it.
    sealed class StalledBroker
    {
        readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        volatile bool _cancelled;

        internal Task Started => _started.Task;

        internal bool Cancelled => _cancelled;

        internal Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> RequestAsync =>
            (_, _, ct) => StallAsync(ct);

        async ValueTask<ReadOnlyMemory<byte>> StallAsync(CancellationToken ct)
        {
            _started.TrySetResult();

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                _cancelled = true;
                throw;
            }


            return ReadOnlyMemory<byte>.Empty;
        }
    }
}
