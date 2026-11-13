using System.Text;

namespace Cntryl.Fitz.Testing.Tests;

public sealed class InMemoryKvLifecycleTests
{
    const string Route = "kv://tenant/app/entities";
    const uint TransactionNotFound = 1050;

    [Fact]
    public async Task ShouldNotifyMatchingSubscribersGivenSeed()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var subscription = await client.SubscribeAsync("kv://tenant/**");
        await using var enumerator = subscription.GetAsyncEnumerator();

        // Act
        client.Seed(Route, "a"u8.ToArray(), "1"u8.ToArray());
        client.Seed(Route, new KvPair("b"u8.ToArray(), "2"u8.ToArray()), new KvPair("c"u8.ToArray(), "3"u8.ToArray()));
        client.Seed("kv://other/app/entities", "z"u8.ToArray(), "9"u8.ToArray());
        await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        var single = enumerator.Current;
        await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        var batch = enumerator.Current;

        // Assert
        Assert.Equal(new KvNotification(Route, 1), single);
        Assert.Equal(new KvNotification(Route, 2), batch);
    }

    [Fact]
    public async Task ShouldSeedFromEnumerableGivenSequenceOfPairs()
    {
        // Arrange
        var client = new InMemoryKvClient();
        IEnumerable<KvPair> pairs = new List<KvPair>
        {
            new("a"u8.ToArray(), "1"u8.ToArray()),
            new("b"u8.ToArray(), "2"u8.ToArray()),
        };
        await using var subscription = await client.SubscribeAsync(Route);
        await using var enumerator = subscription.GetAsyncEnumerator();

        // Act
        client.Seed(Route, pairs);
        await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal(["a", "b"], client.Snapshot(Route).Select(static pair => Encoding.UTF8.GetString(pair.Key.Span)));
        Assert.Equal(2UL, enumerator.Current.MutationCount);
    }

    [Fact]
    public async Task ShouldNotifyRemovedKeyCountGivenClearOfPopulatedRoute()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.Seed(Route, new KvPair("a"u8.ToArray(), ReadOnlyMemory<byte>.Empty), new KvPair("b"u8.ToArray(), ReadOnlyMemory<byte>.Empty));
        await using var subscription = await client.SubscribeAsync(Route);
        await using var enumerator = subscription.GetAsyncEnumerator();

        // Act
        client.Clear(Route);
        client.Clear(Route);
        await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal(2UL, enumerator.Current.MutationCount);
        Assert.Empty(client.Snapshot(Route));
    }

    [Fact]
    public async Task ShouldNotNotifyGivenSeedOfNoPairs()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var subscription = await client.SubscribeAsync(Route);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await using var enumerator = subscription.GetAsyncEnumerator(timeout.Token);

        // Act
        client.Seed(Route, Array.Empty<KvPair>());
        var next = enumerator.MoveNextAsync().AsTask();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
    }

    [Fact]
    public async Task ShouldReportRollbackFaultAndKeepWriteLockGivenExplicitRollback()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Rollback, "BACKEND_ERROR");
        var transaction = await client.BeginAsync(Route);

        // Act
        var rollback = () => transaction.RollbackAsync();
        var begin = () => client.BeginAsync(Route);

        // Assert
        Assert.Equal("BACKEND_ERROR", (await Assert.ThrowsAsync<KvException>(rollback)).Code);
        Assert.Equal(FitzErrorCodes.KvIsolationConflict, (await Assert.ThrowsAsync<KvException>(begin)).DomainCode);
        await transaction.RollbackAsync();
        await using var next = await client.BeginAsync(Route);
        Assert.Equal(Route, next.Route);
    }

    [Fact]
    public async Task ShouldConsumeFaultAndKeepWriteLockGivenFitzFailureDuringDispose()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Rollback, "BACKEND_ERROR");
        var transaction = await client.BeginAsync(Route);

        // Act
        await transaction.DisposeAsync();
        var begin = () => client.BeginAsync(Route);

        // Assert
        Assert.Equal(2, client.Operations.Count(static operation => operation.Operation is KvTestOperation.Begin or KvTestOperation.Rollback));
        Assert.Equal(FitzErrorCodes.KvIsolationConflict, (await Assert.ThrowsAsync<KvException>(begin)).DomainCode);
    }

    [Fact]
    public async Task ShouldPropagateFaultGivenNonWireFailureDuringDispose()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Rollback, new InvalidOperationException("planned"));
        var transaction = await client.BeginAsync(Route);

        // Act
        var dispose = () => transaction.DisposeAsync().AsTask();

        // Assert
        Assert.Equal("planned", (await Assert.ThrowsAsync<InvalidOperationException>(dispose)).Message);
        await transaction.DisposeAsync();
    }

    [Fact]
    public async Task ShouldNotTakeWriteLockGivenFaultedBegin()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Begin, "BACKEND_ERROR");
        await Assert.ThrowsAsync<KvException>(() => client.BeginAsync(Route));

        // Act
        await using var transaction = await client.BeginAsync(Route);

        // Assert
        Assert.Equal(Route, transaction.Route);
    }

    [Fact]
    public async Task ShouldDiscardTransactionsAndReleaseLocksGivenExpiry()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var writer = await client.BeginAsync(Route);
        var other = await client.BeginAsync("kv://tenant/app/other");

        // Act
        var expired = client.ExpireOpenTransactions(Route);
        var get = () => writer.GetAsync("a"u8.ToArray());
        await using var replacement = await client.BeginAsync(Route);

        // Assert
        Assert.Equal(1, expired);
        Assert.Equal(TransactionNotFound, (await Assert.ThrowsAsync<KvException>(get)).DomainCode);
        await other.PutAsync("a"u8.ToArray(), ReadOnlyMemory<byte>.Empty);
        await other.CommitAsync(KvDurability.Sync);
        await writer.DisposeAsync();
    }

    [Fact]
    public async Task ShouldReleaseWriteLocksGivenReset()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var stale = await client.BeginAsync(Route);

        // Act
        client.Reset();
        await using var fresh = await client.BeginAsync(Route);

        // Assert
        Assert.Equal(1, client.Operations.Single().TransactionId);
    }

    [Theory]
    [InlineData(14)]
    [InlineData(1024 * 1024 + 1)]
    public void ShouldRejectFrameSizeOutsideProtocolBoundsGivenOptions(int maxFrameSize)
    {
        // Arrange
        var options = new InMemoryKvClientOptions { MaxFrameSize = maxFrameSize };

        // Act
        var create = () => new InMemoryKvClient(options);

        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(create);
    }

    [Fact]
    public async Task ShouldEnforceConfiguredFrameSizeGivenSmallerLimit()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { MaxFrameSize = 128 });
        await using var transaction = await client.BeginAsync(Route);

        // Act
        var put = () => transaction.PutAsync("k"u8.ToArray(), new byte[128]);

        // Assert
        var error = await Assert.ThrowsAsync<ProtocolException>(put);
        Assert.Contains("limit 128", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldRecordOperationBytesAsMemoryGivenHistory()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route);

        // Act
        await transaction.PutAsync("key"u8.ToArray(), "value"u8.ToArray());
        await transaction.DeleteRangeAsync("a"u8.ToArray(), "z"u8.ToArray());
        var put = client.Operations[1];
        var range = client.Operations[2];

        // Assert
        Assert.Equal("key"u8.ToArray(), put.Key!.Value.ToArray());
        Assert.Equal("value"u8.ToArray(), put.Value!.Value.ToArray());
        Assert.Null(put.EndKey);
        Assert.Equal("z"u8.ToArray(), range.EndKey!.Value.ToArray());
    }
}
