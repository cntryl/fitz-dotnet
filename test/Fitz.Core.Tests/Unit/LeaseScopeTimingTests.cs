using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseScopeTimingTests
{
    const string Route = "lease://prod/app/lock";
    static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);
    static readonly TimeSpan RenewalInterval = TimeSpan.FromSeconds(10);
    static readonly TimeSpan RenewalBudget = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ShouldRenewOncePerIntervalGivenManualClockWhenCallbackOutlivesInterval()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var broker = new FakeLeaseBroker(_ => ValueTask.FromResult<ReadOnlyMemory<byte>>(Renewed()));
        await using var leases = broker.CreateClient(time);
        var release = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = leases.WithLeaseAsync(Route, Ttl, (_, _) => new ValueTask<int>(release.Task));

        // Act
        await time.WaitForTimerAsync(RenewalInterval);
        var renewalsBeforeInterval = broker.Renewals;
        time.Advance(RenewalInterval);
        await broker.WaitForRenewalsAsync(1);
        await time.WaitForTimerAsync(RenewalInterval);
        time.Advance(RenewalInterval);
        await broker.WaitForRenewalsAsync(2);
        release.SetResult(42);
        var result = await pending.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.Equal(0, renewalsBeforeInterval);
        Assert.Equal(42, result);
        Assert.Equal(2, broker.Renewals);
        Assert.Equal(1, broker.Releases);
    }

    [Fact]
    public async Task ShouldKeepCallbackRunningGivenDefiniteRejectionWithSlackWhenRenewing()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var broker = new FakeLeaseBroker(attempt => ValueTask.FromResult<ReadOnlyMemory<byte>>(
            attempt == 1 ? DefiniteRejection() : Renewed()));
        await using var leases = broker.CreateClient(time);
        var release = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackToken = CancellationToken.None;
        var pending = leases.WithLeaseAsync(Route, Ttl, (_, ct) =>
        {
            callbackToken = ct;
            return new ValueTask<int>(release.Task);
        });

        // Act
        await time.WaitForTimerAsync(RenewalInterval);
        time.Advance(RenewalInterval);
        await broker.WaitForRenewalsAsync(1);
        await time.WaitForTimerAsync(RenewalInterval);
        time.Advance(RenewalInterval);
        await broker.WaitForRenewalsAsync(2);
        release.SetResult(7);
        var result = await pending.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.Equal(7, result);
        Assert.False(callbackToken.IsCancellationRequested);
        Assert.Equal(1, broker.Releases);
    }

    [Fact]
    public async Task ShouldLoseLeaseAndCancelCallbackGivenRejectionWithoutSlackWhenRenewing()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var broker = new FakeLeaseBroker(_ => ValueTask.FromResult<ReadOnlyMemory<byte>>(DefiniteRejection()));
        await using var leases = broker.CreateClient(time);
        var pending = leases.WithLeaseAsync(Route, Ttl, async (_, ct) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return 0;
        });

        // Act
        await time.WaitForTimerAsync(RenewalInterval);
        time.Advance(RenewalInterval);
        await broker.WaitForRenewalsAsync(1);
        await time.WaitForTimerAsync(RenewalInterval);
        time.Advance(RenewalInterval);
        var error = await Assert.ThrowsAsync<LeaseException>(() => pending.WaitAsync(TimeSpan.FromSeconds(5)));

        // Assert
        Assert.Equal("LEASE_LOST", error.Code);
        Assert.Equal(2, broker.Renewals);
        Assert.Equal(0, broker.Releases);
    }

    [Fact]
    public async Task ShouldLoseLeaseGivenRenewalOutlastsBudgetWhenBrokerStalls()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var stalled = new TaskCompletionSource<ReadOnlyMemory<byte>>();
        var broker = new FakeLeaseBroker(_ => new ValueTask<ReadOnlyMemory<byte>>(stalled.Task), honorCancellation: true);
        await using var leases = broker.CreateClient(time);
        var callbackCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = leases.WithLeaseAsync(Route, Ttl, async (_, ct) =>
        {
            await using (ct.Register(callbackCancelled.SetResult))
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return 0;
        });

        // Act
        await time.WaitForTimerAsync(RenewalInterval);
        time.Advance(RenewalInterval);
        await broker.WaitForRenewalsAsync(1);
        await time.WaitForTimerAsync(RenewalBudget);
        time.Advance(RenewalBudget);
        var error = await Assert.ThrowsAsync<LeaseException>(() => pending.WaitAsync(TimeSpan.FromSeconds(5)));

        // Assert
        Assert.Equal("LEASE_LOST", error.Code);
        Assert.IsAssignableFrom<OperationCanceledException>(error.InnerException);
        Assert.True(callbackCancelled.Task.IsCompleted);
    }

    static byte[] Acquired(ulong token)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        writer.WriteU8(0);
        writer.WriteU64(token);
        return writer.Build();
    }

    static byte[] Renewed()
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        writer.WriteU64(77);
        return writer.Build();
    }

    // Error class 5 with effect 0: the broker did not apply the renewal and it may be retried.
    static byte[] DefiniteRejection()
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        TestErrorEnvelope.Write(writer, 5005);
        writer.WriteString("renewal rejected");
        return writer.Build();
    }

    sealed class FakeLeaseBroker(Func<int, ValueTask<ReadOnlyMemory<byte>>> renew, bool honorCancellation = false)
    {
        int _renewals;
        int _releases;

        internal int Renewals => Volatile.Read(ref _renewals);

        internal int Releases => Volatile.Read(ref _releases);

        internal LeaseClient CreateClient(TimeProvider time) => new(RequestAsync, timeProvider: time);

        internal async Task WaitForRenewalsAsync(int count)
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (Renewals < count)
                await Task.Delay(TimeSpan.FromMilliseconds(1), deadline.Token);
        }

        async ValueTask<ReadOnlyMemory<byte>> RequestAsync(ushort messageType, ReadOnlyMemory<byte> payload, CancellationToken ct)
        {
            switch (messageType)
            {
                case MessageTypes.LeaseAcquire:
                    return Acquired(77);
                case MessageTypes.LeaseRenew:
                    var attempt = Interlocked.Increment(ref _renewals);
                    return honorCancellation
                        ? await renew(attempt).AsTask().WaitAsync(ct)
                        : await renew(attempt);
                case MessageTypes.LeaseRelease:
                    Interlocked.Increment(ref _releases);
                    return new byte[] { 0 };
                default:
                    throw new InvalidOperationException($"Unexpected message type {messageType}");
            }
        }
    }
}
