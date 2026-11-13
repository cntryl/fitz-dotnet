using System.Text;

namespace Cntryl.Fitz.Testing.Tests;

/// <summary>
/// Pins <see cref="InMemoryKvClient"/> to the observable behavior of the Fitz broker. Each
/// expectation cites the broker source it mirrors (repository cntryl/fitz).
/// </summary>
public sealed class InMemoryKvBrokerFidelityTests
{
    const string Route = "kv://tenant/app/entities";
    const uint TransactionNotFound = 1050;
    const uint WriteInReadOnly = 1054;
    const uint KeyExists = 1055;
    const uint InvalidRequest = 1002;

    // src/domains/kv/actor/scan.rs scan_bounds: forward is start <= key < end.
    [Fact]
    public async Task ShouldBeginAtStartAndStopBeforeEndGivenForwardScan()
    {
        // Arrange
        var client = SeededClient("a", "b", "c", "d", "e");
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery(Bytes("b"), Bytes("d")));

        // Assert
        Assert.Equal(["b", "c"], Keys(page));
    }

    // src/domains/kv/actor/scan.rs scan_bounds: reverse is end < key <= start.
    [Fact]
    public async Task ShouldTreatStartAsInclusiveUpperBoundAndEndAsExclusiveLowerBoundGivenReverseScan()
    {
        // Arrange
        var client = SeededClient("a", "b", "c", "d", "e");
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery(Bytes("d"), Bytes("a"), Reverse: true));

        // Assert
        Assert.Equal(["d", "c", "b"], Keys(page));
    }

    // src/domains/kv/actor/scan.rs scan_bounds: bounds inverted for the direction return empty.
    [Theory]
    [InlineData(false, "d", "b")]
    [InlineData(false, "b", "b")]
    [InlineData(true, "b", "d")]
    [InlineData(true, "b", "b")]
    public async Task ShouldReturnEmptyPageGivenBoundsInvertedForDirection(bool reverse, string start, string end)
    {
        // Arrange
        var client = SeededClient("a", "b", "c", "d", "e");
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery(Bytes(start), Bytes(end), Reverse: reverse));

        // Assert
        Assert.Empty(page.Pairs);
        Assert.False(page.HasMore);
    }

    // docs/clients/spec/kv.md "start_exclusive (bit 1)".
    [Fact]
    public async Task ShouldExcludeStartKeyGivenForwardExclusiveScan()
    {
        // Arrange
        var client = SeededClient("a", "b", "c");
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery(Bytes("a"), StartExclusive: true));

        // Assert
        Assert.Equal(["b", "c"], Keys(page));
    }

    [Fact]
    public async Task ShouldExcludeStartKeyGivenReverseExclusiveScan()
    {
        // Arrange
        var client = SeededClient("a", "b", "c");
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery(Bytes("c"), Reverse: true, StartExclusive: true));

        // Assert
        Assert.Equal(["b", "a"], Keys(page));
    }

    // The real KvTransaction reports support unless the negotiated session lacks it.
    [Fact]
    public async Task ShouldReportExclusiveScanSupportGivenOpenTransaction()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route);

        // Act
        var supported = transaction.SupportsExclusiveScan;

        // Assert
        Assert.True(supported);
    }

    [Fact]
    public async Task ShouldReturnEveryKeyOnceGivenReverseScanAllAcrossPages()
    {
        // Arrange
        var client = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 2 });
        foreach (var key in new[] { "a", "b", "c", "d", "e" })
        {
            client.Seed(Route, Bytes(key), ReadOnlyMemory<byte>.Empty);
        }
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var keys = new List<string>();
        await foreach (var pair in transaction.ScanAllAsync(new KvScanQuery(Reverse: true)))
        {
            keys.Add(Encoding.UTF8.GetString(pair.Key.Span));
        }

        // Assert
        Assert.Equal(["e", "d", "c", "b", "a"], keys);
    }

    // src/domains/kv/actor/scan.rs MAX_SCAN_ITEMS: an omitted limit is not unlimited.
    [Fact]
    public async Task ShouldCapPageAtBrokerItemLimitGivenNoLimitAndLargeRoute()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.Seed(Route, Enumerable.Range(0, 1500)
            .Select(static index => new KvPair(Bytes($"k{index:D5}"), (byte[])[1]))
            .ToArray());
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery());

        // Assert
        Assert.Equal(1024, page.Pairs.Count);
        Assert.True(page.HasMore);
    }

    // src/domains/kv/scan_wire_budget.rs: a response carries at most 65,529 bytes of pairs.
    [Fact]
    public async Task ShouldEndPageAtResponseByteBudgetGivenLargeValues()
    {
        // Arrange
        var client = new InMemoryKvClient();
        for (var index = 0; index < 4; index++)
        {
            client.Seed(Route, Bytes($"k{index}"), new byte[30_000]);
        }
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var page = await transaction.ScanAsync(new KvScanQuery());

        // Assert
        Assert.Equal(2, page.Pairs.Count);
        Assert.True(page.HasMore);
    }

    // src/domains/kv/actor/scan.rs collect_scan_items: a pair that cannot fit any response.
    [Fact]
    public async Task ShouldRejectScanGivenSinglePairLargerThanResponseBudget()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.Seed(Route, Bytes("big"), new byte[70_000]);
        await using var transaction = await client.BeginAsync(Route, KvMode.ReadOnly);

        // Act
        var scan = () => transaction.ScanAsync(new KvScanQuery());

        // Assert
        var error = await Assert.ThrowsAsync<KvException>(scan);
        Assert.Equal("SCAN_FAILED", error.Code);
        Assert.Equal(InvalidRequest, error.DomainCode);
    }

    // src/domains/kv/sink/transactions.rs handle_begin_read_write: the second writer fails at BEGIN.
    [Fact]
    public async Task ShouldRejectSecondReadWriteBeginGivenOpenWriterOnRoute()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var writer = await client.BeginAsync(Route);

        // Act
        var second = () => client.BeginAsync(Route);

        // Assert
        var error = await Assert.ThrowsAsync<KvException>(second);
        Assert.Equal("BEGIN_FAILED", error.Code);
        Assert.Equal(FitzErrorCodes.KvIsolationConflict, error.DomainCode);
    }

    [Fact]
    public async Task ShouldAllowReadOnlyAndOtherRouteBeginsGivenOpenWriterOnRoute()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var writer = await client.BeginAsync(Route);

        // Act
        await using var reader = await client.BeginAsync(Route, KvMode.ReadOnly);
        await using var other = await client.BeginAsync("kv://tenant/app/other");

        // Assert
        Assert.Equal(Route, reader.Route);
        Assert.Equal("kv://tenant/app/other", other.Route);
    }

    [Fact]
    public async Task ShouldReleaseWriteLockGivenCommitRollbackOrDispose()
    {
        // Arrange
        var client = new InMemoryKvClient();

        // Act
        await using (var committed = await client.BeginAsync(Route))
        {
            await committed.CommitAsync(KvDurability.Sync);
        }
        await using (var rolledBack = await client.BeginAsync(Route))
        {
            await rolledBack.RollbackAsync();
        }
        await using (var abandoned = await client.BeginAsync(Route))
        {
        }
        await using var next = await client.BeginAsync(Route);

        // Assert
        Assert.Equal(Route, next.Route);
    }

    // src/domains/kv/actor/transactions.rs handle_commit: the transaction is removed from the
    // actor before the storage commit runs, so a failed commit leaves nothing to retry or roll back.
    [Fact]
    public async Task ShouldConsumeTransactionAndReleaseLockGivenFailedCommit()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(KvTestOperation.Commit, "BACKEND_ERROR");
        var transaction = await client.BeginAsync(Route);
        await transaction.PutAsync(Bytes("a"), Bytes("1"));

        // Act
        var commit = () => transaction.CommitAsync(KvDurability.Sync);
        await Assert.ThrowsAsync<KvException>(commit);
        var get = () => transaction.GetAsync(Bytes("a"));
        var rollback = () => transaction.RollbackAsync();
        var retry = () => transaction.CommitAsync(KvDurability.Sync);

        // Assert
        Assert.Equal(TransactionNotFound, (await Assert.ThrowsAsync<KvException>(get)).DomainCode);
        Assert.Equal(TransactionNotFound, (await Assert.ThrowsAsync<KvException>(rollback)).DomainCode);
        Assert.Equal(TransactionNotFound, (await Assert.ThrowsAsync<KvException>(retry)).DomainCode);
        Assert.Empty(client.Snapshot(Route));
        await transaction.DisposeAsync();
        await using var next = await client.BeginAsync(Route);
        Assert.Equal(Route, next.Route);
    }

    // src/domains/kv/sink/transactions.rs handle_commit_frame releases the lock on any removal.
    [Fact]
    public async Task ShouldConsumeTransactionGivenIsolationConflictAtCommit()
    {
        // Arrange
        var client = new InMemoryKvClient();
        var transaction = await client.BeginAsync(Route);
        await transaction.PutAsync(Bytes("a"), Bytes("1"));
        client.Seed(Route, Bytes("outside"), Bytes("2"));

        // Act
        var commit = () => transaction.CommitAsync(KvDurability.Sync);
        var error = await Assert.ThrowsAsync<KvException>(commit);
        var get = () => transaction.GetAsync(Bytes("a"));

        // Assert
        Assert.Equal(FitzErrorCodes.KvIsolationConflict, error.DomainCode);
        Assert.Equal(TransactionNotFound, (await Assert.ThrowsAsync<KvException>(get)).DomainCode);
        Assert.DoesNotContain(client.Snapshot(Route), static pair => pair.Key.Span.SequenceEqual("a"u8));
        await using var next = await client.BeginAsync(Route);
        Assert.Equal(Route, next.Route);
        await transaction.DisposeAsync();
    }

    // docs/clients/spec/error-codes.md 1005: BUSY means the request was never accepted.
    [Fact]
    public async Task ShouldKeepTransactionRetryableGivenBusyCommitFault()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.FailNext(
            KvTestOperation.Commit,
            new KvException("busy", "COMMIT_FAILED", 1, FitzErrorCodes.KvBusy));
        await using var transaction = await client.BeginAsync(Route);
        await transaction.PutAsync(Bytes("a"), Bytes("1"));

        // Act
        await Assert.ThrowsAsync<KvException>(() => transaction.CommitAsync(KvDurability.Sync));
        await transaction.CommitAsync(KvDurability.Sync);

        // Assert
        Assert.Single(client.Snapshot(Route));
    }

    // src/domains/kv/actor/mutations.rs handle_delete / handle_delete_range increment the
    // mutation count unconditionally; src/domains/kv/sink/transactions.rs notifies when it is > 0.
    [Fact]
    public async Task ShouldCountDeleteOfMissingKeyAndEmptyRangeAsMutationsGivenCommit()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var subscription = await client.SubscribeAsync(Route);
        await using var transaction = await client.BeginAsync(Route);

        // Act
        await transaction.DeleteAsync(Bytes("missing"));
        await transaction.DeleteRangeAsync(Bytes("a"), Bytes("z"));
        await transaction.CommitAsync(KvDurability.Sync);
        await using var enumerator = subscription.GetAsyncEnumerator();
        await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal(2UL, enumerator.Current.MutationCount);
    }

    [Fact]
    public async Task ShouldNotNotifyGivenCommitWithoutMutations()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var subscription = await client.SubscribeAsync(Route);
        await using var transaction = await client.BeginAsync(Route);
        await transaction.GetAsync(Bytes("a"));
        await transaction.CommitAsync(KvDurability.Sync);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await using var enumerator = subscription.GetAsyncEnumerator(timeout.Token);

        // Act
        var next = enumerator.MoveNextAsync().AsTask();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
    }

    // src/domains/kv/actor/mutations.rs handle_delete_range: start must be less than end.
    [Theory]
    [InlineData("b", "b")]
    [InlineData("c", "b")]
    public async Task ShouldRejectDeleteRangeGivenStartNotBelowEnd(string start, string end)
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route);

        // Act
        var delete = () => transaction.DeleteRangeAsync(Bytes(start), Bytes(end));

        // Assert
        var error = await Assert.ThrowsAsync<KvException>(delete);
        Assert.Equal("DELETE_RANGE_FAILED", error.Code);
        Assert.Equal(InvalidRequest, error.DomainCode);
    }

    // src/protocol/kv_codec/frame_and_routes.rs kv_error_code: 1054 and 1055.
    [Fact]
    public async Task ShouldShapeBrokerErrorsLikeRealClientGivenReadOnlyWriteAndDuplicateInsert()
    {
        // Arrange
        var client = new InMemoryKvClient();
        client.Seed(Route, Bytes("a"), Bytes("1"));
        await using var reader = await client.BeginAsync(Route, KvMode.ReadOnly);
        await using var writer = await client.BeginAsync(Route);

        // Act
        var put = await Assert.ThrowsAsync<KvException>(() => reader.PutAsync(Bytes("a"), Bytes("2")));
        var insert = await Assert.ThrowsAsync<KvException>(() => writer.InsertAsync(Bytes("a"), Bytes("2")));

        // Assert
        Assert.Equal("PUT_FAILED", put.Code);
        Assert.Equal(WriteInReadOnly, put.DomainCode);
        Assert.Equal("INSERT_FAILED", insert.Code);
        Assert.Equal(KeyExists, insert.DomainCode);
    }

    // The broker has no key or value size cap and accepts an empty key; only the frame is bounded.
    [Fact]
    public async Task ShouldAcceptEmptyKeyAndEmptyValueGivenPut()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route);

        // Act
        await transaction.PutAsync(ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
        await transaction.CommitAsync(KvDurability.Sync);

        // Assert
        Assert.True(client.Read(Route, ReadOnlyMemory<byte>.Empty).Found);
    }

    // src/Fitz.Core/Connection/FitzConnection.cs EncodeFrame: frame (15-byte header + payload) vs limit.
    [Fact]
    public async Task ShouldRejectWriteGivenEncodedFrameLargerThanFrameLimit()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route);
        var value = new byte[FitzLimits.MaxFrameSize];

        // Act
        var put = () => transaction.PutAsync(Bytes("k"), value);

        // Assert
        await Assert.ThrowsAsync<ProtocolException>(put);
        Assert.DoesNotContain(client.Operations, static operation => operation.Operation == KvTestOperation.Put);
    }

    [Fact]
    public async Task ShouldAcceptWriteGivenEncodedFrameExactlyAtFrameLimit()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var transaction = await client.BeginAsync(Route);
        var overhead = FitzLimits.MinFrameSize + 8 + 4 + Encoding.UTF8.GetByteCount(Route) + 4 + 1 + 4;
        var value = new byte[FitzLimits.MaxFrameSize - overhead];

        // Act
        await transaction.PutAsync(Bytes("k"), value);

        // Assert
        Assert.Contains(client.Operations, static operation => operation.Operation == KvTestOperation.Put);
    }

    // Order of delivery must follow order of commit even when committers race.
    [Fact]
    public async Task ShouldNotifyInCommitOrderGivenConcurrentCommitsOnDifferentRoutes()
    {
        // Arrange
        var client = new InMemoryKvClient();
        await using var subscription = await client.SubscribeAsync("kv://tenant/app/*");
        const int writers = 32;
        using var start = new ManualResetEventSlim();

        // Act
        var tasks = Enumerable.Range(0, writers).Select(index => Task.Run(async () =>
        {
            await using var transaction = await client.BeginAsync($"kv://tenant/app/r{index:D2}");
            for (var key = 0; key <= index; key++)
            {
                await transaction.PutAsync(new[] { (byte)key }, ReadOnlyMemory<byte>.Empty);
            }
            start.Wait();
            await transaction.CommitAsync(KvDurability.Buffered);
        })).ToArray();
        await Task.Delay(50);
        start.Set();
        await Task.WhenAll(tasks);
        var committed = client.Operations
            .Where(static operation => operation.Operation == KvTestOperation.Commit)
            .Select(static operation => operation.Route)
            .ToArray();
        var delivered = new List<string>();
        await using var enumerator = subscription.GetAsyncEnumerator();
        while (delivered.Count < writers)
        {
            Assert.True(await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
            delivered.Add(enumerator.Current.Route);
        }

        // Assert
        Assert.Equal(committed, delivered);
    }

    static InMemoryKvClient SeededClient(params string[] keys)
    {
        var client = new InMemoryKvClient();
        foreach (var key in keys)
        {
            client.Seed(Route, Bytes(key), ReadOnlyMemory<byte>.Empty);
        }
        return client;
    }

    static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    static string[] Keys(KvScanResult result) =>
        result.Pairs.Select(static pair => Encoding.UTF8.GetString(pair.Key.Span)).ToArray();
}
