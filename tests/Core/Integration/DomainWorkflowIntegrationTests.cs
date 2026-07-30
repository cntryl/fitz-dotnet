using System.Text;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class DomainWorkflowIntegrationTests
{
    [Fact]
    public async Task should_return_not_found_given_missing_key_when_get_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv().BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"), KvMode.ReadOnly);

        var result = await tx.GetAsync("missing"u8.ToArray());

        Assert.False(result.Found);
        Assert.Null(result.Value);
    }

    [Fact]
    public async Task should_delay_visibility_given_nonzero_delay_when_queue_reserved()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await client.Queue().EnqueueAsync(route, "delayed"u8.ToArray(), delayMs: 2_000);

        var early = await client.Queue().ReserveAsync(route, leaseSeconds: 30, batchSize: 1);

        Assert.Empty(early);
        await Task.Delay(TimeSpan.FromMilliseconds(2_100));
        var visible = await client.Queue().ReserveAsync(route, leaseSeconds: 30, batchSize: 1);
        var item = Assert.Single(visible);
        Assert.Equal("delayed", Encoding.UTF8.GetString(item.Body.Span));
        await item.CompleteAsync();
    }

    [Fact]
    public async Task should_isolate_realms_given_staging_subscription_when_prod_published()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var staging = IntegrationFixture.CreateUniqueRoute("notice");
        var prod = IntegrationFixture.CreateUniqueRoute("notice");
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var subscription = await client.Notice().SubscribeAsync(staging, (_, _) =>
        {
            received.TrySetResult();
            return ValueTask.CompletedTask;
        });

        await client.Notice().PublishAsync(prod, "prod"u8.ToArray());

        await Assert.ThrowsAsync<TimeoutException>(() => received.Task.WaitAsync(TimeSpan.FromMilliseconds(300)));
    }

    [Fact]
    public async Task should_reject_invalid_cron_given_malformed_syntax_when_schedule_created()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        await Assert.ThrowsAsync<ScheduleException>(async () =>
            await client.Schedule().CreateAsync(
                IntegrationFixture.CreateUniqueRoute("schedule"),
                "not a cron",
                ScheduleDeliveryMode.Broadcast,
                ReadOnlyMemory<byte>.Empty));
    }

    [Fact]
    public async Task should_return_empty_given_offset_beyond_watermark_when_stream_read()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var records = new List<StreamRecord>();

        await foreach (var record in client.Stream().ReadAsync(IntegrationFixture.CreateUniqueRoute("stream"), 999_999, 10))
        {
            records.Add(record);
        }

        Assert.Empty(records);
    }

    [Fact]
    public async Task should_reject_duplicate_insert_given_existing_key_when_insert_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv().BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"));
        await tx.InsertAsync("key"u8.ToArray(), "first"u8.ToArray());

        await Assert.ThrowsAsync<KvException>(() => tx.InsertAsync("key"u8.ToArray(), "second"u8.ToArray()));
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task should_reject_write_given_read_only_mode_when_put_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv().BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"), KvMode.ReadOnly);

        await Assert.ThrowsAsync<KvException>(() => tx.PutAsync("key"u8.ToArray(), "value"u8.ToArray()));
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task should_reject_inverted_bounds_given_invalid_range_when_scan_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv().BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"));

        await Assert.ThrowsAsync<KvException>(async () =>
        {
            await foreach (var _ in tx.ScanAsync(new KvScanQuery { StartKey = "z"u8.ToArray(), EndKey = "a"u8.ToArray() }))
            {
            }
        });
        await tx.RollbackAsync();
    }

    [Fact]
    public async Task should_reject_second_commit_given_completed_transaction_when_commit_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var tx = await client.Kv().BeginAsync(IntegrationFixture.CreateUniqueRoute("kv"));
        await tx.CommitAsync();

        await Assert.ThrowsAsync<KvException>(() => tx.CommitAsync());
    }

    [Fact]
    public async Task should_redeliver_given_expired_reservation_when_reserve_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await client.Queue().EnqueueAsync(route, "retry"u8.ToArray());
        var first = Assert.Single(await client.Queue().ReserveAsync(route, leaseSeconds: 1, batchSize: 1));

        await Task.Delay(TimeSpan.FromMilliseconds(1_100));
        var second = Assert.Single(await client.Queue().ReserveAsync(route, leaseSeconds: 30, batchSize: 1));

        Assert.Equal(first.Body, second.Body);
        await second.CompleteAsync();
    }

    [Fact]
    public async Task should_reject_completion_given_wrong_token_when_complete_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await client.Queue().EnqueueAsync(route, "token"u8.ToArray());
        var item = Assert.Single(await client.Queue().ReserveAsync(route, leaseSeconds: 30, batchSize: 1));

        await Assert.ThrowsAsync<QueueException>(() => item.CompleteWithTokenAsync(ulong.MaxValue));
        await item.CompleteAsync();
    }

    [Fact]
    public async Task should_reject_completion_given_expired_reservation_when_complete_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await client.Queue().EnqueueAsync(route, "expired"u8.ToArray());
        var item = Assert.Single(await client.Queue().ReserveAsync(route, leaseSeconds: 1, batchSize: 1));

        await Task.Delay(TimeSpan.FromMilliseconds(1_100));

        await Assert.ThrowsAsync<QueueException>(() => item.CompleteAsync());
        var redelivered = Assert.Single(await client.Queue().ReserveAsync(route, leaseSeconds: 30, batchSize: 1));
        await redelivered.CompleteAsync();
    }

    [Fact]
    public Task should_match_single_segment_wildcard_given_single_star_subscription_when_notice_published()
    {
        return AssertWildcardDeliveryAsync(multiSegment: false);
    }

    [Fact]
    public Task should_match_multi_segment_wildcard_given_double_star_subscription_when_notice_published()
    {
        return AssertWildcardDeliveryAsync(multiSegment: true);
    }

    private static async Task AssertWildcardDeliveryAsync(bool multiSegment)
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("notice");
        var parts = route.Split('/');
        var pattern = multiSegment
            ? $"{parts[0]}//{parts[2]}/*/*"
            : $"{parts[0]}//{parts[2]}/{parts[3]}/*";
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var subscription = await client.Notice().SubscribeAsync(pattern, (_, _) =>
        {
            received.TrySetResult();
            return ValueTask.CompletedTask;
        });

        await client.Notice().PublishAsync(route, "matched"u8.ToArray());

        await received.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task should_stop_delivery_given_active_subscription_when_dispose_precedes_publish()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("notice");
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await client.Notice().SubscribeAsync(route, (_, _) =>
        {
            received.TrySetResult();
            return ValueTask.CompletedTask;
        });
        await subscription.DisposeAsync();

        await client.Notice().PublishAsync(route, "ignored"u8.ToArray());

        await Assert.ThrowsAsync<TimeoutException>(() => received.Task.WaitAsync(TimeSpan.FromMilliseconds(300)));
    }

    [Fact]
    public async Task should_reject_acquire_given_held_lease_when_acquire_called()
    {
        await using var owner = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await using var contender = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await owner.ConnectAsync();
        await contender.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("lease");
        using var lease = await owner.Lease().AcquireAsync(route, 30);

        await Assert.ThrowsAsync<LeaseException>(async () => await contender.Lease().AcquireAsync(route, 30));
        await lease.ReleaseAsync();
    }

    [Fact]
    public async Task should_reject_append_given_mismatched_expected_offset_when_append_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var session = await client.Stream().BeginAsync(IntegrationFixture.CreateUniqueRoute("stream"));

        await Assert.ThrowsAsync<StreamException>(() => session.AppendAsync(42, "mismatch"u8.ToArray()));
        await session.RollbackAsync();
    }

    [Fact]
    public async Task should_discard_writes_given_open_session_when_rollback_called()
    {
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        var session = await client.Stream().BeginAsync(route);
        await session.AppendAsync(0, "discarded"u8.ToArray());
        await session.RollbackAsync();
        var records = new List<StreamRecord>();

        await foreach (var record in client.Stream().ReadAsync(route, 0, 10))
        {
            records.Add(record);
        }

        Assert.Empty(records);
    }

    [Fact]
    public async Task should_round_trip_rpc_response_given_registered_worker()
    {
        var route = IntegrationFixture.CreateUniqueRoute("rpc");
        await using var workerClient = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await using var callerClient = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await workerClient.ConnectAsync();
        await callerClient.ConnectAsync();

        await using var registration = await workerClient.Rpc().RegisterWorkerAsync(route, async (request, writer, ct) =>
        {
            Assert.Equal("ping", Encoding.UTF8.GetString(request.Body.Span));
            await writer.SendAsync("pong"u8.ToArray(), isEnd: true, ct);
        });

        var responses = new List<string>();
        await foreach (var response in callerClient.Rpc().CallAsync(route, "ping"u8.ToArray()))
        {
            responses.Add(Encoding.UTF8.GetString(response.Body.Span));
        }

        Assert.Equal("pong", Assert.Single(responses));
    }

    [Fact]
    public async Task should_deliver_notice_given_subscribe_publish_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("notice");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var received = new TaskCompletionSource<(string Route, string Body)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using var subscription = await client.Notice().SubscribeAsync(route, (message, _) =>
        {
            received.TrySetResult((message.Route, Encoding.UTF8.GetString(message.Body.Span)));
            return ValueTask.CompletedTask;
        });

        await client.Notice().PublishAsync(route, "notice-body"u8.ToArray());
        var delivered = await received.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(route, delivered.Route);
        Assert.Equal("notice-body", delivered.Body);
    }

    [Fact]
    public async Task should_round_trip_queue_message_given_enqueue_reserve_complete_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("queue");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var id = await client.Queue().EnqueueAsync(route, "queue-body"u8.ToArray());
        Assert.NotEqual((ulong)0, id);

        var items = await client.Queue().ReserveAsync(route, leaseSeconds: 30, batchSize: 1, waitSeconds: 1);
        Assert.Single(items);
        Assert.Equal("queue-body", Encoding.UTF8.GetString(items[0].Body.Span));

        await items[0].CompleteAsync();
    }

    [Fact]
    public async Task should_round_trip_stream_records_given_begin_append_commit_read_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var session = await client.Stream().BeginAsync(route);
        await session.AppendAsync(0, "one"u8.ToArray());
        await session.AppendAsync(1, "two"u8.ToArray());
        await session.CommitAsync();

        var records = new List<string>();
        await foreach (var record in client.Stream().ReadAsync(route, startOffset: 0, limit: 10))
        {
            records.Add(Encoding.UTF8.GetString(record.Body));
        }

        Assert.True(records.Count >= 2, $"expected at least 2 committed stream records, got {records.Count}");
        Assert.Contains("one", records);
        Assert.Contains("two", records);
    }

    [Fact]
    public async Task should_round_trip_filtered_stream_records_given_discriminator_filter_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("stream");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var session = await client.Stream().BeginAsync(route);
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
        await foreach (var record in client.Stream().ReadAsync(route, startOffset: 0, limit: 10, filter: filter))
        {
            records.Add(Encoding.UTF8.GetString(record.Body));
        }

        var page = await client.Stream().ReadPageAsync(route, startOffset: 0, limit: 10, filter: filter);

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
    public async Task should_hold_then_release_lease_given_acquire_extend_release_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("lease");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var lease = await client.Lease().AcquireAsync(route, ttlSecs: 30);
        Assert.Equal(route, lease.Route);

        var held = await client.Lease().QueryAsync(route);
        Assert.True(held.IsHeld);

        await lease.ExtendAsync(45);
        await lease.ReleaseAsync();

        var released = await WaitForLeaseReleaseAsync(client, route, TimeSpan.FromSeconds(2));
        Assert.False(released.IsHeld);
    }

    [Fact]
    public async Task should_create_and_cancel_schedule_given_valid_cron_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("schedule");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var id = await client.Schedule().CreateAsync(route, "*/5 * * * *", ScheduleDeliveryMode.Broadcast, "schedule-body"u8.ToArray());
        Assert.False(string.IsNullOrWhiteSpace(id));

        // Dotnet API currently cancels by route.
        await client.Schedule().CancelAsync(route);
    }

    [Fact]
    public async Task should_write_then_read_kv_value_given_transaction_commit_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("kv");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var tx = await client.Kv().BeginAsync(route);
        await tx.PutAsync("k"u8.ToArray(), "v"u8.ToArray());
        await tx.CommitAsync();

        var read = await client.Kv().BeginAsync(route, KvMode.ReadOnly);
        var result = await read.GetAsync("k"u8.ToArray());

        Assert.True(result.Found);
        Assert.Equal("v", Encoding.UTF8.GetString(result.Value!.Value.Span));
    }

    private static async Task<Cntryl.Fitz.Abstractions.Domains.Lease.LeaseInfo> WaitForLeaseReleaseAsync(
        Client client,
        string route,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var info = await client.Lease().QueryAsync(route);
            if (!info.IsHeld)
            {
                return info;
            }

            await Task.Delay(100);
        }

        return await client.Lease().QueryAsync(route);
    }
}
