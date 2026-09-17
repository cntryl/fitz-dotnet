using Cntryl.Fitz.Testing;

namespace Cntryl.Fitz.Testing.Tests;

public sealed class InMemoryKvClientTests
{
    const string Route = "kv://tenant/app/entities";

    [Fact]
    public async Task ShouldExposeOwnWritesAndPersistOnlyCommittedChangesGivenTransactionLifecycle()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.Seed(Route, "a"u8.ToArray(), "original"u8.ToArray());

        // Act
        await using (var rolledBack = await client.BeginAsync(Route, KvDurability.Async))
        {
            await rolledBack.PutAsync("a"u8.ToArray(), "temporary"u8.ToArray());
            Assert.Equal("temporary"u8.ToArray(), (await rolledBack.GetAsync("a"u8.ToArray())).Value!.Value.ToArray());
        }
        await using (var committed = await client.BeginAsync(Route, KvDurability.Sync))
        {
            await committed.PutAsync("b"u8.ToArray(), "saved"u8.ToArray());
            await committed.CommitAsync();
        }

        // Assert
        Assert.Collection(
            client.Snapshot(Route),
            pair => Assert.Equal("original"u8.ToArray(), pair.Value.ToArray()),
            pair => Assert.Equal("saved"u8.ToArray(), pair.Value.ToArray()));
    }

    [Fact]
    public void ShouldKeepRoutesIsolatedAndCloneBytesGivenSeedAndSnapshot()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var key = "key"u8.ToArray();
        var value = "value"u8.ToArray();

        // Act
        client.Seed(Route, key, value);
        key[0] = 0;
        value[0] = 0;
        var snapshot = client.Snapshot(Route);
        snapshot[0].Value.ToArray()[0] = 0;

        // Assert
        Assert.Equal("key"u8.ToArray(), client.Snapshot(Route)[0].Key.ToArray());
        Assert.Equal("value"u8.ToArray(), client.Snapshot(Route)[0].Value.ToArray());
        Assert.Empty(client.Snapshot("kv://other/app/entities"));
    }

    [Fact]
    public async Task ShouldRejectWritesGivenReadOnlyTransaction()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadOnly);

        // Act
        var put = () => transaction.PutAsync("a"u8.ToArray(), "value"u8.ToArray());
        var delete = () => transaction.DeleteAsync("a"u8.ToArray());

        // Assert
        Assert.Equal("READ_ONLY", (await Assert.ThrowsAsync<KvException>(put)).Code);
        Assert.Equal("READ_ONLY", (await Assert.ThrowsAsync<KvException>(delete)).Code);
    }

    [Fact]
    public async Task ShouldApplyInsertAndDeleteRangeGivenWritableTransaction()
    {
        // Arrange
        var client = new InMemoryKvClient();
        foreach (var key in new[] { "a", "b", "c", "d" })
        {
            client.Seed(Route, System.Text.Encoding.UTF8.GetBytes(key), "value"u8.ToArray());
        }
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);

        // Act
        var duplicate = await Record.ExceptionAsync(() =>
            transaction.InsertAsync("a"u8.ToArray(), "other"u8.ToArray()));
        await transaction.DeleteRangeAsync("b"u8.ToArray(), "d"u8.ToArray());
        await transaction.CommitAsync();

        // Assert
        Assert.Equal("KEY_EXISTS", Assert.IsType<KvException>(duplicate).Code);
        Assert.Equal(["a", "d"], client.Snapshot(Route).Select(static pair => System.Text.Encoding.UTF8.GetString(pair.Key.Span)));
    }

    [Fact]
    public async Task ShouldPageForwardAndReverseWithinHalfOpenBoundsGivenBrokerPageCap()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 2 });
        foreach (var key in new[] { "a", "b", "c", "d", "e" })
        {
            client.Seed(Route, System.Text.Encoding.UTF8.GetBytes(key), Array.Empty<byte>());
        }
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadOnly);

        // Act
        var forward = await transaction.ScanAsync(new KvScanQuery("b"u8.ToArray(), "e"u8.ToArray(), Limit: 10));
        var reverse = await transaction.ScanAsync(new KvScanQuery("b"u8.ToArray(), "e"u8.ToArray(), Limit: 10, Reverse: true));

        // Assert
        Assert.Equal(["b", "c"], TextKeys(forward));
        Assert.True(forward.HasMore);
        Assert.Equal(["d", "c"], TextKeys(reverse));
        Assert.True(reverse.HasMore);
    }

    [Theory]
    [InlineData(false, "a,b,c,d,e")]
    [InlineData(true, "e,d,c,b,a")]
    public async Task ShouldOwnExclusivePageResumptionGivenScanAll(bool reverse, string expected)
    {
        // Arrange
        ArgumentNullException.ThrowIfNull(expected);
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 2 });
        foreach (var key in new[] { "a", "b", "c", "d", "e" })
        {
            client.Seed(Route, System.Text.Encoding.UTF8.GetBytes(key), Array.Empty<byte>());
        }
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadOnly);

        // Act
        var keys = new List<string>();
        await foreach (var pair in transaction.ScanAllAsync(new KvScanQuery(Reverse: reverse)))
        {
            keys.Add(System.Text.Encoding.UTF8.GetString(pair.Key.Span));
        }

        // Assert
        Assert.Equal(expected.Split(','), keys);
    }

    [Fact]
    public async Task ShouldRejectSecondWriterGivenConcurrentRouteMutation()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var winner = await client.BeginAsync(Route, KvDurability.Async);
        await using var loser = await client.BeginAsync(Route, KvDurability.Async);
        await winner.PutAsync("a"u8.ToArray(), Array.Empty<byte>());
        await loser.PutAsync("b"u8.ToArray(), Array.Empty<byte>());

        // Act
        await winner.CommitAsync();
        var commit = () => loser.CommitAsync();

        // Assert
        var error = await Assert.ThrowsAsync<KvException>(commit);
        Assert.Equal(FitzErrorCodes.KvIsolationConflict, error.DomainCode);
    }

    [Fact]
    public async Task ShouldNotifyMatchingSubscribersGivenSuccessfulCommit()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var subscription = await client.SubscribeAsync("kv://tenant/**");
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);

        // Act
        await transaction.PutAsync("a"u8.ToArray(), Array.Empty<byte>());
        await transaction.DeleteAsync("missing"u8.ToArray());
        await transaction.CommitAsync();
        await using var enumerator = subscription.GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));

        // Assert
        Assert.Equal(Route, enumerator.Current.Route);
        Assert.Equal(2UL, enumerator.Current.MutationCount);
    }

    [Fact]
    public async Task ShouldInjectOneMatchingFailureAndRetainOperationHistoryGivenQueuedFault()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Put, new KvException("planned", "BACKEND_ERROR"), Route);
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);

        // Act
        var first = () => transaction.PutAsync("a"u8.ToArray(), Array.Empty<byte>());
        var error = await Assert.ThrowsAsync<KvException>(first);
        await transaction.PutAsync("a"u8.ToArray(), Array.Empty<byte>());

        // Assert
        Assert.Equal("BACKEND_ERROR", error.Code);
        Assert.Equal(
            [KvTestOperation.Begin, KvTestOperation.Put, KvTestOperation.Put],
            client.Operations.Select(static operation => operation.Operation));
        Assert.Equal("a"u8.ToArray(), client.Operations[^1].Key);
    }

    [Fact]
    public async Task ShouldClearQueuedFaultsGivenReusableFixture()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Begin, "BACKEND_ERROR", route: Route);

        // Act
        client.ClearFaults();
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);

        // Assert
        Assert.Equal(KvTestOperation.Begin, client.Operations.Single().Operation);
    }

    [Fact]
    public async Task ShouldCloneReturnedOperationBytesGivenHistoryInspection()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);
        await transaction.PutAsync("key"u8.ToArray(), "value"u8.ToArray());

        // Act
        var operation = client.Operations[^1];
        Assert.IsType<byte[]>(operation.Key)[0] = 0;
        Assert.IsType<byte[]>(operation.Value)[0] = 0;

        // Assert
        Assert.Equal("key"u8.ToArray(), client.Operations[^1].Key);
        Assert.Equal("value"u8.ToArray(), client.Operations[^1].Value);
    }

    [Fact]
    public async Task ShouldExposeClonedTransactionAndScanDetailsGivenDirectoryStyleRead()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var start = "team\0"u8.ToArray();
        byte[] end = [.. "team"u8, 1];
        await using var transaction = await client.BeginAsync(
            Route,
            KvDurability.Sync,
            KvMode.ReadOnly);

        // Act
        await transaction.ScanAsync(new KvScanQuery(start, end, Limit: 25, Reverse: true));
        start[0] = 0;
        end[0] = 0;
        var begin = client.Operations[0];
        var scan = client.Operations[1];

        // Assert
        Assert.NotNull(begin.TransactionId);
        Assert.Equal(begin.TransactionId, scan.TransactionId);
        Assert.Equal(KvDurability.Sync, begin.Durability);
        Assert.Equal(KvMode.ReadOnly, begin.Mode);
        Assert.Equal("team\0"u8.ToArray(), scan.ScanQuery!.StartKey!.Value.ToArray());
        Assert.Equal([.. "team"u8, 1], scan.ScanQuery.EndKey!.Value.ToArray());
        Assert.Equal(25U, scan.ScanQuery.Limit);
        Assert.True(scan.ScanQuery.Reverse);
    }

    [Fact]
    public void ShouldMakeFixtureReuseConciseGivenBatchSeedReadAndReset()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.Seed(
            Route,
            new KvPair("a"u8.ToArray(), "one"u8.ToArray()),
            new KvPair("b"u8.ToArray(), "two"u8.ToArray()));

        // Act
        var found = client.Read(Route, "b"u8.ToArray());
        client.Reset();

        // Assert
        Assert.True(found.Found);
        Assert.Equal("two"u8.ToArray(), found.Value!.Value.ToArray());
        Assert.Empty(client.Snapshot(Route));
        Assert.Empty(client.Operations);
    }

    [Fact]
    public async Task ShouldCreateStructuredFailureGivenCodeOnlyFaultSetup()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Begin, "BACKEND_ERROR", route: Route);

        // Act
        var begin = () => client.BeginAsync(Route, KvDurability.Async);

        // Assert
        Assert.Equal("BACKEND_ERROR", (await Assert.ThrowsAsync<KvException>(begin)).Code);
    }

    [Fact]
    public async Task ShouldHonorCancellationBeforeChangingStateGivenCancelledOperation()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        // Act
        var put = () => transaction.PutAsync("a"u8.ToArray(), Array.Empty<byte>(), cancellation.Token);

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(put);
        Assert.Single(client.Operations);
    }

    [Fact]
    public async Task ShouldTreatZeroLimitAsUnboundedGivenBrokerPageCap()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 2 });
        client.Seed(
            Route,
            new KvPair("a"u8.ToArray(), Array.Empty<byte>()),
            new KvPair("b"u8.ToArray(), Array.Empty<byte>()),
            new KvPair("c"u8.ToArray(), Array.Empty<byte>()));
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery(Limit: 0));

        // Assert
        Assert.Equal(["a", "b"], TextKeys(page));
        Assert.True(page.HasMore);
    }

    [Fact]
    public async Task ShouldReturnNoMoreGivenExactBrokerPageBoundary()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 2 });
        client.Seed(
            Route,
            new KvPair("a"u8.ToArray(), Array.Empty<byte>()),
            new KvPair("b"u8.ToArray(), Array.Empty<byte>()));
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery());

        // Assert
        Assert.Equal(["a", "b"], TextKeys(page));
        Assert.False(page.HasMore);
    }

    [Fact]
    public async Task ShouldExposeDeleteRangeBoundsGivenRecordedOperation()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);

        // Act
        await transaction.DeleteRangeAsync("a"u8.ToArray(), "z"u8.ToArray());
        var operation = client.Operations[^1];

        // Assert
        Assert.Equal("a"u8.ToArray(), operation.Key);
        Assert.Equal("z"u8.ToArray(), operation.EndKey);
        Assert.Null(operation.Value);
    }

    [Fact]
    public async Task ShouldInvalidateOldTransactionsAndRestartIdentifiersGivenReset()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var oldTransaction = await client.BeginAsync(Route, KvDurability.Async);
        client.Reset();

        // Act
        var staleWrite = () => oldTransaction.PutAsync("stale"u8.ToArray(), Array.Empty<byte>());
        var error = await Assert.ThrowsAsync<KvException>(staleWrite);
        await using var newTransaction = await client.BeginAsync(Route, KvDurability.Async);

        // Assert
        Assert.Equal("TX_CLOSED", error.Code);
        Assert.Equal(1, client.Operations.Single().TransactionId);
        Assert.Empty(client.Snapshot(Route));
    }

    [Fact]
    public async Task ShouldKeepOriginalSnapshotGivenConcurrentCommit()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.Seed(Route, "key"u8.ToArray(), "old"u8.ToArray());
        await using var reader = await client.BeginAsync(Route, KvDurability.Async, KvMode.ReadOnly);
        await using var writer = await client.BeginAsync(Route, KvDurability.Async);

        // Act
        await writer.PutAsync("key"u8.ToArray(), "new"u8.ToArray());
        await writer.CommitAsync();
        var read = await reader.GetAsync("key"u8.ToArray());

        // Assert
        Assert.Equal("old"u8.ToArray(), read.Value!.Value.ToArray());
        Assert.Equal("new"u8.ToArray(), client.Read(Route, "key"u8.ToArray()).Value!.Value.ToArray());
    }

    [Fact]
    public async Task ShouldDiscardExplicitRollbackAndRejectFurtherUseGivenWritableTransaction()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);
        await transaction.PutAsync("key"u8.ToArray(), "value"u8.ToArray());

        // Act
        await transaction.RollbackAsync();
        var read = () => transaction.GetAsync("key"u8.ToArray());

        // Assert
        Assert.Equal("TX_CLOSED", (await Assert.ThrowsAsync<KvException>(read)).Code);
        Assert.False(client.Read(Route, "key"u8.ToArray()).Found);
        Assert.Equal(KvTestOperation.Rollback, client.Operations[^1].Operation);
    }

    [Fact]
    public async Task ShouldNotifyOnlyMatchingSubscriptionsGivenCommittedMutation()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var matching = await client.SubscribeAsync("kv://tenant/**");
        await using var other = await client.SubscribeAsync("kv://other/**");
        await using var transaction = await client.BeginAsync(Route, KvDurability.Async);

        // Act
        await transaction.PutAsync("key"u8.ToArray(), Array.Empty<byte>());
        await transaction.CommitAsync();
        await using var matchingEnumerator = matching.GetAsyncEnumerator();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await using var otherEnumerator = other.GetAsyncEnumerator(timeout.Token);
        var matched = await matchingEnumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        var unmatched = otherEnumerator.MoveNextAsync().AsTask();

        // Assert
        Assert.True(matched);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => unmatched);
    }

    [Fact]
    public async Task ShouldSurfaceBackpressureGivenBoundedSubscriptionBuffer()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions
        {
            SubscriptionBufferCapacity = 1,
        });
        await using var subscription = await client.SubscribeAsync("kv://tenant/**");

        // Act
        for (var index = 0; index < 2; index++)
        {
            await using var transaction = await client.BeginAsync(Route, KvDurability.Async);
            await transaction.PutAsync(new byte[] { (byte)index }, Array.Empty<byte>());
            await transaction.CommitAsync();
        }
        await using var enumerator = subscription.GetAsyncEnumerator();
        var received = await enumerator.MoveNextAsync();
        var overflow = enumerator.MoveNextAsync().AsTask();

        // Assert
        Assert.True(received);
        await Assert.ThrowsAsync<SubscriptionBackpressureException>(() => overflow);
    }

    [Theory]
    [InlineData(0, null)]
    [InlineData(null, 0)]
    public void ShouldRejectNonpositiveCapacityGivenClientOptions(int? scanPageSize, int? subscriptionCapacity)
    {
        // Arrange
        var options = new InMemoryKvClientOptions
        {
            ScanPageSize = scanPageSize,
            SubscriptionBufferCapacity = subscriptionCapacity,
        };

        // Act
        var create = () => new InMemoryKvClient(options);

        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(create);
    }

    [Theory]
    [InlineData("kv://tenant/area")]
    [InlineData("kv://tenant/area/resource/extra")]
    [InlineData("kv://tenant/*/resource")]
    [InlineData("queue://tenant/area/resource")]
    public async Task ShouldRejectInvalidRouteGivenBegin(string route)
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        var begin = () => client.BeginAsync(route, KvDurability.Async);

        // Assert
        Assert.Equal("INVALID_ROUTE", (await Assert.ThrowsAsync<KvException>(begin)).Code);
    }

    static string[] TextKeys(KvScanResult result) =>
        result.Pairs.Select(static pair => System.Text.Encoding.UTF8.GetString(pair.Key.Span)).ToArray();
}
