using Cntryl.Fitz.Abstractions.Domains.Lease;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseClientCompatibilityTests
{
    [Fact]
    public async Task ShouldPreserveOneArgumentCallbacksGivenLegacyClientWhenManagedLeaseRuns()
    {
        // Arrange
        ILeaseClient client = new LegacyLeaseClient();
        var nonGenericInvoked = false;

        var result = await client.WithLeaseAsync(
            "lease://prod/app/generic",
            30,
            cancellationToken => ValueTask.FromResult(!cancellationToken.IsCancellationRequested));

        // Act
        await client.WithLeaseAsync(
            "lease://prod/app/non-generic",
            30,
            cancellationToken =>
            {
                nonGenericInvoked = !cancellationToken.IsCancellationRequested;
                return ValueTask.CompletedTask;
            });


        // Assert
        Assert.True(result);
        Assert.True(nonGenericInvoked);
    }

    [Fact]
    public async Task ShouldFailClearlyGivenAuthorityCallbackOnLegacyCustomClientWhenManagedLeaseRuns()
    {
        // Arrange
        // Act
        // Assert
        ILeaseClient client = new LegacyLeaseClient();

        var generic = await Assert.ThrowsAsync<NotSupportedException>(() => client.WithLeaseAsync(
            "lease://prod/app/generic",
            30,
            static (authority, _) => ValueTask.FromResult(authority.FencingToken)));
        var nonGeneric = await Assert.ThrowsAsync<NotSupportedException>(() => client.WithLeaseAsync(
            "lease://prod/app/non-generic",
            30,
            static (_, _) => ValueTask.CompletedTask));

        Assert.Equal(ILeaseClient.AuthorityCallbacksNotSupportedMessage, generic.Message);
        Assert.Equal(ILeaseClient.AuthorityCallbacksNotSupportedMessage, nonGeneric.Message);
    }

    sealed class LegacyLeaseClient : ILeaseClient
    {
        public Task<ILease> AcquireAsync(
            string route,
            ulong ttlSecs,
            uint waitSeconds = 0,
            CancellationToken ct = default) => Task.FromException<ILease>(new NotSupportedException());

        public async Task<T> WithLeaseAsync<T>(
            string route,
            ulong ttlSecs,
            Func<CancellationToken, ValueTask<T>> callback,
            LeaseExecutionOptions? options = null,
            CancellationToken ct = default) => await callback(ct);

        public async Task WithLeaseAsync(
            string route,
            ulong ttlSecs,
            Func<CancellationToken, ValueTask> callback,
            LeaseExecutionOptions? options = null,
            CancellationToken ct = default) => await callback(ct);

        public Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default) => Task.FromException<LeaseInfo>(new NotSupportedException());

        public Task<LeaseSubscription> SubscribeAsync(
            string route,
            CancellationToken ct = default) => Task.FromException<LeaseSubscription>(new NotSupportedException());
    }
}
