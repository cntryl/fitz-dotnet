using System.Text;
using Cntryl.Fitz.Abstractions.Domains.Kv;

namespace Cntryl.Fitz.Core.Tests.Integration;

public sealed class DomainWorkflowIntegrationTests
{
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
    public async Task should_hold_then_release_lease_given_acquire_extend_release_workflow()
    {
        var route = IntegrationFixture.CreateUniqueRoute("lease");
        await using var client = IntegrationFixture.CreateAnonymousClient(IntegrationFixture.GetAnonymousWebSocketUrl());
        await client.ConnectAsync();

        var lease = await client.Lease().AcquireAsync(route, ttlSecs: 30);
        Assert.NotEqual((ulong)0, lease.Token);

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

        var id = await client.Schedule().CreateAsync(route, "*/5 * * * *", "schedule-body"u8.ToArray());
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
