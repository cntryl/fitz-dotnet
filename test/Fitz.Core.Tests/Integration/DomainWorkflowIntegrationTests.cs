using System.Text;
using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class DomainWorkflowIntegrationTests
{
    [Fact]
    public async Task ShouldDeliverExactRouteGivenWildcardKvSubscriptionWhenTransactionCommits()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var uniqueParts = IntegrationFixture.CreateUniqueRoute("kv").Split('/');
        var route = $"{uniqueParts[0]}//{uniqueParts[2]}/{uniqueParts[^1]}/resource";
        var pattern = $"{uniqueParts[0]}//{uniqueParts[2]}/{uniqueParts[^1]}/**";
        await using var subscription = await client.Kv.SubscribeAsync(pattern);
        var received = ReadFirstAsync(subscription);

        // Act
        var tx = await client.Kv.BeginAsync(route, Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        await tx.PutAsync("key"u8.ToArray(), "value"u8.ToArray());
        await tx.CommitAsync();
        var notification = await received.WaitAsync(TimeSpan.FromSeconds(2));

        // Assert
        Assert.Equal(route, notification.Route);
        Assert.Equal((ulong)1, notification.MutationCount);
    }

    [Fact]
    public async Task ShouldReturnNotFoundGivenMissingKeyWhenGetCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv.BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"), Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async, KvMode.ReadOnly);


        // Act
        var result = await tx.GetAsync("missing"u8.ToArray());


        // Assert
        Assert.False(result.Found);
        Assert.Null(result.Value);
    }

    [Fact]
    public async Task ShouldDelayVisibilityGivenNonzeroDelayWhenQueueReserved()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await client.Queue.EnqueueAsync(route, "delayed"u8.ToArray(), delayMs: 2_000);


        // Act
        var early = await client.Queue.ReserveAsync(route, leaseSeconds: 30, batchSize: 1);


        // Assert
        Assert.Empty(early);
        await Task.Delay(TimeSpan.FromMilliseconds(2_100));
        var visible = await client.Queue.ReserveAsync(route, leaseSeconds: 30, batchSize: 1);
        var item = Assert.Single(visible);
        Assert.Equal("delayed", Encoding.UTF8.GetString(item.Body.Span));
        await item.CompleteAsync();
    }

    [Fact]
    public async Task ShouldIsolateRealmsGivenStagingSubscriptionWhenProdPublished()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var staging = IntegrationFixture.CreateUniqueRoute("notice").Replace("conformance-realm", "staging-realm", StringComparison.Ordinal);
        var prod = IntegrationFixture.CreateUniqueRoute("notice");
        await using var subscription = await client.Notice.SubscribeAsync(staging);
        var received = ReadFirstAsync(subscription);


        // Act
        await client.Notice.PublishAsync(prod, "prod"u8.ToArray());


        // Assert
        await Assert.ThrowsAsync<TimeoutException>(() => received.WaitAsync(TimeSpan.FromMilliseconds(300)));
    }

    [Fact]
    public async Task ShouldRejectInvalidCronGivenMalformedSyntaxWhenScheduleCreated()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());

        // Act
        await client.ConnectAsync();


        // Assert
        await Assert.ThrowsAsync<ScheduleException>(async () =>
            await client.Schedule.CreateAsync(
                IntegrationFixture.CreateUniqueRoute("schedule"),
                "not a cron",
                ScheduleDeliveryMode.Broadcast,
                ReadOnlyMemory<byte>.Empty));
    }

    [Fact]
    public async Task ShouldReturnEmptyGivenOffsetBeyondWatermarkWhenStreamRead()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var records = new List<StreamRecord>();


        // Act
        await foreach (var record in client.Stream.ReadAsync(IntegrationFixture.CreateUniqueRoute("stream"), 999_999, 10))
        {
            records.Add(record);
        }

        // Assert
        Assert.Empty(records);
    }

    [Fact]
    public async Task ShouldRejectDuplicateInsertGivenExistingKeyWhenInsertCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv.BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"), Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);

        // Act
        await tx.InsertAsync("key"u8.ToArray(), "first"u8.ToArray());


        // Assert
        await Assert.ThrowsAsync<KvException>(() => tx.InsertAsync("key"u8.ToArray(), "second"u8.ToArray()));
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task ShouldRejectWriteGivenReadOnlyModeWhenPutCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        // Act
        var tx = await client.Kv.BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"), Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async, KvMode.ReadOnly);


        // Assert
        await Assert.ThrowsAsync<KvException>(() => tx.PutAsync("key"u8.ToArray(), "value"u8.ToArray()));
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task ShouldReturnEmptyPageGivenInvertedBoundsWhenScanCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv.BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"), Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);


        // Act
        var result = await tx.ScanAsync(new KvScanQuery { StartKey = "z"u8.ToArray(), EndKey = "a"u8.ToArray() });


        // Assert
        Assert.Empty(result.Pairs);
        Assert.False(result.HasMore);
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task ShouldRejectSecondCommitGivenCompletedTransactionWhenCommitCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv.BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"), Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);

        // Act
        await tx.CommitAsync();


        // Assert
        await Assert.ThrowsAsync<KvException>(() => tx.CommitAsync());
    }

    [Fact]
    public async Task ShouldRedeliverGivenExpiredReservationWhenReserveCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");

        // Act
        await client.Queue.EnqueueAsync(route, "retry"u8.ToArray());

        // Assert
        var first = Assert.Single(await client.Queue.ReserveAsync(route, leaseSeconds: 1, batchSize: 1));

        await Task.Delay(TimeSpan.FromMilliseconds(1_100));
        var second = Assert.Single(await client.Queue.ReserveAsync(route, leaseSeconds: 30, batchSize: 1));

        Assert.Equal(first.Body, second.Body);
        await second.CompleteAsync();
    }

    [Fact]
    public async Task ShouldRejectCompletionGivenWrongTokenWhenCompleteCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");

        // Act
        await client.Queue.EnqueueAsync(route, "token"u8.ToArray());

        // Assert
        var item = Assert.Single(await client.Queue.ReserveAsync(route, leaseSeconds: 30, batchSize: 1));

        await Assert.ThrowsAsync<QueueException>(() => item.CompleteWithTokenAsync(ulong.MaxValue));
        await item.CompleteAsync();
    }

    [Fact]
    public async Task ShouldRejectCompletionGivenExpiredReservationWhenCompleteCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");

        // Act
        await client.Queue.EnqueueAsync(route, "expired"u8.ToArray());

        // Assert
        var item = Assert.Single(await client.Queue.ReserveAsync(route, leaseSeconds: 1, batchSize: 1));

        await Task.Delay(TimeSpan.FromMilliseconds(1_100));

        await Assert.ThrowsAsync<QueueException>(() => item.CompleteAsync());
        var redelivered = Assert.Single(await client.Queue.ReserveAsync(route, leaseSeconds: 30, batchSize: 1));
        await redelivered.CompleteAsync();
    }

    [Fact]
    public Task ShouldMatchSingleSegmentWildcardGivenSingleStarSubscriptionWhenNoticePublished() => AssertWildcardDeliveryAsync(multiSegment: false);

    [Fact]
    public Task ShouldMatchMultiSegmentWildcardGivenDoubleStarSubscriptionWhenNoticePublished() => AssertWildcardDeliveryAsync(multiSegment: true);

    static async Task AssertWildcardDeliveryAsync(bool multiSegment)
    {
        // Arrange
        // Act
        // Assert
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("notice");
        var parts = route.Split('/');
        var pattern = multiSegment
            ? $"{parts[0]}//{parts[2]}/*/*"
            : $"{parts[0]}//{parts[2]}/{parts[3]}/*";
        await using var subscription = await client.Notice.SubscribeAsync(pattern);
        var received = ReadFirstAsync(subscription);

        await client.Notice.PublishAsync(route, "matched"u8.ToArray());

        _ = await received.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task ShouldStopDeliveryGivenActiveSubscriptionWhenDisposePrecedesPublish()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("notice");
        var subscription = await client.Notice.SubscribeAsync(route);
        await subscription.DisposeAsync();

        await client.Notice.PublishAsync(route, "ignored"u8.ToArray());


        // Act
        await using var enumerator = subscription.GetAsyncEnumerator();

        // Assert
        Assert.False(await enumerator.MoveNextAsync());
    }

    [Fact]
    public async Task ShouldRejectAcquireGivenHeldLeaseWhenAcquireCalled()
    {
        // Arrange
        await using var owner = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await using var contender = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await owner.ConnectAsync();
        await contender.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("lease");

        // Act
        await using var lease = await owner.Lease.AcquireAsync(route, 30);


        // Assert
        var error = await Assert.ThrowsAsync<LeaseException>(async () => await contender.Lease.AcquireAsync(route, 30));
        Assert.Equal(FitzErrorCodes.LeaseHeld, error.DomainCode);
        await lease.ReleaseAsync();
    }

    [Fact]
    public async Task ShouldRejectAppendGivenMismatchedExpectedOffsetWhenAppendCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        // Act
        var session = await client.Stream.BeginAsync(IntegrationFixture.CreateUniqueRoute("stream"));


        // Assert
        await Assert.ThrowsAsync<StreamException>(() => session.AppendAsync(42, "mismatch"u8.ToArray()));
        await session.RollbackAsync();
    }

    [Fact]
    public async Task ShouldDiscardWritesGivenOpenSessionWhenRollbackCalled()
    {
        // Arrange
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        var session = await client.Stream.BeginAsync(route);
        await session.AppendAsync(0, "discarded"u8.ToArray());
        await session.RollbackAsync();
        var records = new List<StreamRecord>();


        // Act
        await foreach (var record in client.Stream.ReadAsync(route, 0, 10))
        {
            records.Add(record);
        }

        // Assert
        Assert.Empty(records);
    }

    [Fact]
    public async Task ShouldRoundTripRpcResponseGivenRegisteredWorkerWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("rpc");
        await using var workerClient = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await using var callerClient = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await workerClient.ConnectAsync();

        // Act
        await callerClient.ConnectAsync();


        // Assert
        await using var registration = await workerClient.Rpc.RegisterWorkerAsync(route, async (request, writer, ct) =>
        {
            Assert.Equal("ping", Encoding.UTF8.GetString(request.Body.Span));
            await writer.SendAsync("pong"u8.ToArray(), isEnd: true, ct);
        });

        var responses = new List<string>();
        await foreach (var response in callerClient.Rpc.CallAsync(route, "ping"u8.ToArray()))
        {
            responses.Add(Encoding.UTF8.GetString(response.Body.Span));
        }

        Assert.Equal("pong", Assert.Single(responses));
    }

    [Fact]
    public async Task ShouldDeliverNoticeGivenSubscribePublishWorkflowWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("notice");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        await using var subscription = await client.Notice.SubscribeAsync(route);
        var received = ReadFirstAsync(subscription);

        await client.Notice.PublishAsync(route, "notice-body"u8.ToArray());
        var message = await received.WaitAsync(TimeSpan.FromSeconds(2));

        // Act
        var delivered = (message.Route, Body: Encoding.UTF8.GetString(message.Body.Span));


        // Assert
        Assert.Equal(route, delivered.Route);
        Assert.Equal("notice-body", delivered.Body);
    }

    static async Task<T> ReadFirstAsync<T>(IAsyncEnumerable<T> notifications)
    {
        await foreach (var notification in notifications)
        {
            return notification;
        }

        throw new InvalidOperationException("Subscription completed before a notification arrived");
    }

    [Fact]
    public async Task ShouldRoundTripQueueMessageGivenEnqueueReserveCompleteWorkflowWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();


        // Act
        var id = await client.Queue.EnqueueAsync(route, "queue-body"u8.ToArray());

        // Assert
        Assert.NotEqual((ulong)0, id);

        var items = await client.Queue.ReserveAsync(route, leaseSeconds: 30, batchSize: 1, waitSeconds: 1);
        Assert.Single(items);
        Assert.Equal("queue-body", Encoding.UTF8.GetString(items[0].Body.Span));

        await items[0].CompleteAsync();
    }

    [Fact]
    public async Task ShouldRoundTripStreamRecordsGivenBeginAppendCommitReadWorkflowWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var session = await client.Stream.BeginAsync(route);
        await session.AppendAsync(0, "one"u8.ToArray());
        await session.AppendAsync(1, "two"u8.ToArray());
        await session.CommitAsync();

        var records = new List<string>();

        // Act
        await foreach (var record in client.Stream.ReadAsync(route, startOffset: 0, limit: 10))
        {
            records.Add(Encoding.UTF8.GetString(record.Body));
        }

        // Assert
        Assert.True(records.Count >= 2, $"expected at least 2 committed stream records, got {records.Count}");
        Assert.Contains("one", records);
        Assert.Contains("two", records);
    }

    [Fact]
    public async Task ShouldRoundTripFilteredStreamRecordsGivenDiscriminatorFilterWorkflowWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var session = await client.Stream.BeginAsync(route);
        await session.AppendAsync(0, "alpha"u8.ToArray(), discriminator: "proj.alpha");
        await session.AppendAsync(1, "beta"u8.ToArray(), discriminator: "audit.beta");
        await session.CommitAsync();

        var filter = new StreamFilterSet
        {
            Clauses = new[]
            {
                new StreamFilterClause
                {
                    Kind = StreamFilterClauseKind.Equals,
                    Value = "proj.alpha",
                },
            },
        };

        var records = new List<string>();
        await foreach (var record in client.Stream.ReadAsync(route, startOffset: 0, limit: 10, filter: filter))
        {
            records.Add(Encoding.UTF8.GetString(record.Body));
        }

        // Act
        var page = await client.Stream.ReadPageAsync(route, startOffset: 0, limit: 10, filter: filter);


        // Assert
        Assert.Equal("alpha", Assert.Single(records));
        Assert.Equal((ulong)1, page.Cursor.LastResourceOffset);
        Assert.False(page.Cursor.HasMore);
        Assert.Collection(
            page.Items,
            item =>
            {
                Assert.Equal(StreamReadItemKind.Event, item.Kind);
                Assert.NotNull(item.Record);
                Assert.Equal("alpha", Encoding.UTF8.GetString(item.Record!.Body));
            },
            item =>
            {
                Assert.Equal(StreamReadItemKind.Filtered, item.Kind);
                Assert.Equal((ulong)1, item.Offset);
                Assert.Equal(StreamFilteredReason.ServerFilter, item.Reason);
            });
    }

    [Fact]
    public async Task ShouldHoldThenReleaseLeaseGivenAcquireExtendReleaseWorkflowWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("lease");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();


        // Act
        var lease = await client.Lease.AcquireAsync(route, ttlSecs: 30);

        // Assert
        Assert.Equal(route, lease.Route);

        var held = await client.Lease.QueryAsync(route);
        Assert.True(held.IsHeld);

        await lease.ExtendAsync(45);
        await lease.ReleaseAsync();

        var released = await WaitForLeaseReleaseAsync(client, route, TimeSpan.FromSeconds(2));
        Assert.False(released.IsHeld);
    }

    [Fact]
    public async Task ShouldCreateAndCancelScheduleGivenValidCronWorkflowWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("schedule");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();


        // Act
        var id = await client.Schedule.CreateAsync(route, "*/5 * * * *", ScheduleDeliveryMode.Broadcast, "schedule-body"u8.ToArray());

        // Assert
        Assert.False(string.IsNullOrWhiteSpace(id));

        // Dotnet API currently cancels by route.
        await client.Schedule.CancelAsync(route);
    }

    [Fact]
    public async Task ShouldWriteThenReadKvValueGivenTransactionCommitWorkflowWhenWorkflowRuns()
    {
        // Arrange
        var route = IntegrationFixture.CreateUniqueRoute("kv");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var tx = await client.Kv.BeginAsync(route, Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        await tx.PutAsync("k"u8.ToArray(), "v"u8.ToArray());
        await tx.CommitAsync();

        var read = await client.Kv.BeginAsync(route, Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async, KvMode.ReadOnly);

        // Act
        var result = await read.GetAsync("k"u8.ToArray());


        // Assert
        Assert.True(result.Found);
        Assert.Equal("v", Encoding.UTF8.GetString(result.Value!.Value.Span));
    }

    static async Task<Cntryl.Fitz.Abstractions.Domains.Lease.LeaseInfo> WaitForLeaseReleaseAsync(
        Client client,
        string route,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var info = await client.Lease.QueryAsync(route);
            if (!info.IsHeld)
            {
                return info;
            }

            await Task.Delay(100);
        }

        return await client.Lease.QueryAsync(route);
    }
}
