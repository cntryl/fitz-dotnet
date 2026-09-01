using System.Collections.Concurrent;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseInventoryObserverTests
{
    [Fact]
    public async Task should_subscribe_before_listing_and_apply_buffered_notifications_after_first_list_installs()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        var listGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") }, before: () => listGate.Task);
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a"), MakeItem("lease://acme/renderers/b") });

        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);

        // Act
        var observeTask = leaseClient.ObserveAsync("lease://acme/renderers/*");

        await WaitUntilAsync(() => broker.Calls.Contains("SUBSCRIBE") && broker.Calls.Contains("LIST"));
        // The subscribe ack must have landed before the list request was ever issued.
        Assert.Equal("SUBSCRIBE", broker.Calls.First());

        // A notification arrives for a route that is not yet in the first list page.
        broker.PushNotification(broker.LastSubscriptionId, "lease://acme/renderers/b");

        listGate.SetResult();
        await using var observer = await observeTask;

        // Assert
        Assert.True(observer.IsReady);
        Assert.Equal(2, broker.Calls.Count(call => call == "LIST"));
        Assert.True(observer.View.ContainsKey("lease://acme/renderers/b"));
        Assert.True(observer.View.ContainsKey("lease://acme/renderers/a"));
    }

    [Fact]
    public async Task should_relist_on_steady_state_notifications_to_preserve_complete_items()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        broker.QueueListPage(new[]
        {
            MakeItem("lease://acme/renderers/a"),
            MakeItem("lease://acme/renderers/b", "worker-2", incarnation: 72, renewals: 3),
        });
        broker.QueueListPage(new[]
        {
            MakeItem("lease://acme/renderers/b", "worker-2", incarnation: 72, renewals: 3),
        });

        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);

        // Act
        await using var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");
        Assert.True(observer.IsReady);
        var listCallsAfterBootstrap = broker.Calls.Count(call => call == "LIST");

        broker.PushNotification(broker.LastSubscriptionId, "lease://acme/renderers/b");
        await WaitUntilAsync(() => observer.View.ContainsKey("lease://acme/renderers/b"));

        // Assert: LIST, not QUERY, preserves the fields QUERY cannot return.
        Assert.True(broker.Calls.Count(call => call == "LIST") > listCallsAfterBootstrap);
        Assert.DoesNotContain("QUERY", broker.Calls);
        Assert.Equal("worker-2", observer.View["lease://acme/renderers/b"].OwnerId);
        Assert.Equal(72UL, observer.View["lease://acme/renderers/b"].HolderIncarnation);
        Assert.Equal(3U, observer.View["lease://acme/renderers/b"].Renewals);

        // A notification for a route that is no longer held removes it from the view.
        broker.PushNotification(broker.LastSubscriptionId, "lease://acme/renderers/a");
        await WaitUntilAsync(() => !observer.View.ContainsKey("lease://acme/renderers/a"));

        Assert.False(observer.View.ContainsKey("lease://acme/renderers/a"));
    }

    [Fact]
    public async Task should_reconcile_on_a_periodic_interval()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a"), MakeItem("lease://acme/renderers/b") });
        broker.DefaultListPage = new[] { MakeItem("lease://acme/renderers/a"), MakeItem("lease://acme/renderers/b") };

        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);
        var options = new LeaseObserveOptions
        {
            ReconciliationInterval = TimeSpan.FromMilliseconds(20),
            ReconciliationJitterRatio = 0,
        };

        // Act
        await using var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*", options);
        var listCallsAfterBootstrap = broker.Calls.Count(call => call == "LIST");

        // Assert: periodic reconciliation eventually issues another full relist without any notification.
        await WaitUntilAsync(
            () => broker.Calls.Count(call => call == "LIST") > listCallsAfterBootstrap,
            TimeSpan.FromSeconds(2));
        Assert.True(observer.View.ContainsKey("lease://acme/renderers/b"));
    }

    [Fact]
    public async Task should_rebootstrap_from_scratch_when_the_connection_reconnects()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a"), MakeItem("lease://acme/renderers/c") });

        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);

        // Act
        await using var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");
        var subscribeCallsBeforeReconnect = broker.Calls.Count(call => call == "SUBSCRIBE");
        var listCallsBeforeReconnect = broker.Calls.Count(call => call == "LIST");

        await leaseClient.SimulateReconnectAsync();
        await WaitUntilAsync(() => observer.View.ContainsKey("lease://acme/renderers/c"));

        // Assert: a fresh bootstrap re-subscribed and re-listed from scratch.
        Assert.True(broker.Calls.Count(call => call == "SUBSCRIBE") > subscribeCallsBeforeReconnect);
        Assert.True(broker.Calls.Count(call => call == "LIST") > listCallsBeforeReconnect);
        Assert.True(observer.IsReady);
        Assert.True(observer.View.ContainsKey("lease://acme/renderers/c"));
    }

    [Fact]
    public async Task should_stop_background_work_and_unsubscribe_when_disposed()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        broker.DefaultListPage = new[] { MakeItem("lease://acme/renderers/a") };

        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);
        var options = new LeaseObserveOptions
        {
            ReconciliationInterval = TimeSpan.FromMilliseconds(15),
            ReconciliationJitterRatio = 0,
        };

        var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*", options);
        var subscriptionId = broker.LastSubscriptionId;

        // Act
        await observer.DisposeAsync();
        var listCallsAtDisposal = broker.Calls.Count(call => call == "LIST");
        await Task.Delay(100);

        // Assert
        Assert.Contains("UNSUBSCRIBE", broker.Calls);
        Assert.Equal(listCallsAtDisposal, broker.Calls.Count(call => call == "LIST"));

        // A notification arriving after disposal must not resurrect background processing.
        broker.PushNotification(subscriptionId, "lease://acme/renderers/z");
        await Task.Delay(50);
        Assert.False(observer.View.ContainsKey("lease://acme/renderers/z"));
    }

    private static LeaseListItem MakeItem(
        string route,
        string owner = "worker-1",
        ulong incarnation = 1,
        string acquiredAt = "2026-08-29T00:00:00Z",
        ulong expiresInSecs = 30,
        uint renewals = 0)
    {
        return new LeaseListItem(route, owner, incarnation, acquiredAt, expiresInSecs, renewals);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(2));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Condition was not met within the timeout.");
            }

            await Task.Delay(10);
        }
    }

    private sealed class FakeLeaseBroker
    {
        private readonly ConcurrentQueue<(IReadOnlyList<LeaseListItem> Items, LeaseListCursor? Next, Func<Task>? Before)> _listPages = new();
        private long _nextSubscriptionId;

        public ConcurrentQueue<string> Calls { get; } = new();

        public ulong LastSubscriptionId { get; private set; }

        public Action<byte[]>? NotifyHandler { get; private set; }

        public IReadOnlyList<LeaseListItem>? DefaultListPage { get; set; }

        public void QueueListPage(IReadOnlyList<LeaseListItem> items, LeaseListCursor? next = null, Func<Task>? before = null)
        {
            _listPages.Enqueue((items, next, before));
        }

        public async Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken ct)
        {
            if (messageType == MessageTypes.LeaseSubscribe)
            {
                Calls.Enqueue("SUBSCRIBE");
                LastSubscriptionId = (ulong)Interlocked.Increment(ref _nextSubscriptionId);
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU64(LastSubscriptionId);
                return writer.Build();
            }

            if (messageType == MessageTypes.LeaseUnsubscribe)
            {
                Calls.Enqueue("UNSUBSCRIBE");
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                return writer.Build();
            }

            if (messageType == MessageTypes.LeaseList)
            {
                Calls.Enqueue("LIST");
                if (!_listPages.TryDequeue(out var page))
                {
                    page = (DefaultListPage ?? Array.Empty<LeaseListItem>(), null, null);
                }

                if (page.Before is not null)
                {
                    await page.Before().ConfigureAwait(false);
                }

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU32((uint)page.Items.Count);
                foreach (var item in page.Items)
                {
                    writer.WriteString(item.Route);
                    writer.WriteString(item.OwnerId);
                    writer.WriteU64(item.HolderIncarnation);
                    writer.WriteString(item.AcquiredAt);
                    writer.WriteU64(item.ExpiresInSecs);
                    writer.WriteU32(item.Renewals);
                }

                writer.WriteU8((byte)(page.Next is null ? 0 : 1));
                if (page.Next is not null)
                {
                    writer.WriteU64(page.Next.SnapshotId);
                    writer.WriteU32(page.Next.Offset);
                }

                return writer.Build();
            }

            throw new InvalidOperationException($"FakeLeaseBroker received an unexpected message type {messageType}");
        }

        public TestRegistration RegisterNotificationHandler(ushort messageType, Action<byte[]> handler)
        {
            Assert.Equal(MessageTypes.LeaseNotify, messageType);
            NotifyHandler = handler;
            return new TestRegistration();
        }

        public void PushNotification(ulong subscriptionId, string route)
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU64(subscriptionId);
            writer.WriteString(route);
            writer.WriteU32(0);
            NotifyHandler?.Invoke(writer.Build());
        }
    }
}
