using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Stream;

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
