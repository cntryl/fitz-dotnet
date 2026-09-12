namespace Cntryl.Fitz.Core.Tests.Unit;

/// <summary>
/// A custom <see cref="ILeaseClient"/> implements only the authority-aware
/// <c>WithLeaseAsync</c> overloads; the cancellation-only pair is supplied by the interface
/// and must behave identically, discarding the fence.
/// </summary>
public sealed class LeaseClientCompatibilityTests
{
    [Fact]
    public async Task ShouldSupplyOneArgumentCallbacksGivenMinimalClientWhenManagedLeaseRuns()
    {
        // Arrange
        ILeaseClient client = new MinimalLeaseClient();
        var nonGenericInvoked = false;

        var result = await client.WithLeaseAsync(
            "lease://prod/app/generic",
            TimeSpan.FromSeconds(30),
            ct => ValueTask.FromResult(!ct.IsCancellationRequested));

        // Act
        await client.WithLeaseAsync(
            "lease://prod/app/non-generic",
            TimeSpan.FromSeconds(30),
            ct =>
            {
                nonGenericInvoked = !ct.IsCancellationRequested;
                return ValueTask.CompletedTask;
            });

        // Assert
        Assert.True(result);
        Assert.True(nonGenericInvoked);
    }

    [Fact]
    public async Task ShouldReachAuthorityCallbacksGivenMinimalClientWhenManagedLeaseRuns()
    {
        // Arrange
        ILeaseClient client = new MinimalLeaseClient();

        // Act
        var fencingToken = await client.WithLeaseAsync(
            "lease://prod/app/generic",
            TimeSpan.FromSeconds(30),
            static (authority, _) => ValueTask.FromResult(authority.FencingToken));

        // Assert
        Assert.Equal(MinimalLeaseClient.FencingToken, fencingToken);
    }

    [Fact]
    public async Task ShouldRejectNullCallbackGivenSuppliedOverloadWhenManagedLeaseRuns()
    {
        // Arrange
        ILeaseClient client = new MinimalLeaseClient();

        // Act
        // Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.WithLeaseAsync(
            "lease://prod/app/generic",
            TimeSpan.FromSeconds(30),
            (Func<CancellationToken, ValueTask>)null!));
    }

    sealed class MinimalLeaseClient : ILeaseClient
    {
        internal const ulong FencingToken = 42;

        public Task<ILease> AcquireAsync(
            string route,
            TimeSpan ttl,
            TimeSpan wait = default,
            CancellationToken ct = default) => Task.FromException<ILease>(new NotSupportedException());

        public async Task<T> WithLeaseAsync<T>(
            string route,
            TimeSpan ttl,
            Func<LeaseAuthority, CancellationToken, ValueTask<T>> callback,
            LeaseExecutionOptions? options = null,
            CancellationToken ct = default) => await callback(new LeaseAuthority(FencingToken), ct);

        public async Task WithLeaseAsync(
            string route,
            TimeSpan ttl,
            Func<LeaseAuthority, CancellationToken, ValueTask> callback,
            LeaseExecutionOptions? options = null,
            CancellationToken ct = default) => await callback(new LeaseAuthority(FencingToken), ct);

        public Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default) =>
            Task.FromException<LeaseInfo>(new NotSupportedException());

        public Task<LeaseSubscription> SubscribeAsync(
            string route,
            CancellationToken ct = default) => Task.FromException<LeaseSubscription>(new NotSupportedException());

        public Task<LeaseListResult> ListAsync(
            string pattern,
            LeaseListCursor? cursor = null,
            int? limit = null,
            CancellationToken ct = default) => Task.FromException<LeaseListResult>(new NotSupportedException());

        public Task<ILeaseInventoryObserver> ObserveAsync(
            string pattern,
            LeaseObserveOptions? options = null,
            CancellationToken ct = default) => Task.FromException<ILeaseInventoryObserver>(new NotSupportedException());
    }
}
