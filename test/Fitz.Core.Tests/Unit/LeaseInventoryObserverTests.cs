using System.Collections.Concurrent;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseInventoryObserverTests
{
    [Fact]
    public async Task ShouldRejectOptionsGivenNonpositiveListTimeoutWhenObserving()
    {
        var broker = new FakeLeaseBroker();
        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);
        var options = new LeaseObserveOptions { ListTimeout = TimeSpan.Zero };

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            leaseClient.ObserveAsync("lease://acme/renderers/*", options));

        Assert.Empty(broker.Calls);
    }

    [Fact]
    public async Task ShouldReturnTypedTimeoutGivenSlowListPassWhenObserving()
    {
        using var leaseClient = new LeaseClient(
            async (messageType, _, cancellationToken) =>
            {
                if (messageType == MessageTypes.LeaseSubscribe)
                {
                    using var response = new BinaryBufferWriter();
                    response.WriteU8(0);
                    response.WriteU64(1);
                    return response.Build();
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return ReadOnlyMemory<byte>.Empty;
            },
            registerNotificationHandler: (_, _) => new TestRegistration());
        var options = new LeaseObserveOptions { ListTimeout = TimeSpan.FromMilliseconds(20) };

        var error = await Assert.ThrowsAsync<LeaseException>(() =>
            leaseClient.ObserveAsync("lease://acme/renderers/*", options));

        Assert.Equal("LIST_TIMEOUT", error.Code);
    }

    [Fact]
    public async Task ShouldStopAndCompleteUpdatesGivenNormalSubscriptionCompletionWhenClientCloses()
    {
        var broker = new FakeLeaseBroker();
        broker.QueueListPage([MakeItem("lease://acme/renderers/a")]);
        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);
        var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");
        var updatesCompleted = Task.Run(async () =>
        {
            await foreach (var _ in observer.Updates)
            {
            }
        });

        leaseClient.Dispose();

        await updatesCompleted.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.False(observer.IsReady);
        var callsAtShutdown = broker.Calls.Count;
        await Task.Delay(100);
        Assert.Equal(callsAtShutdown, broker.Calls.Count);
        await observer.DisposeAsync();
    }

    [Fact]
    public async Task ShouldBoundConvergenceGivenContinuouslyDirtyInventoryWhenBootstrapping()
    {
        var broker = new FakeLeaseBroker();
        for (var i = 0; i < 10; i++)
        {
            broker.QueueListPage(
                [MakeItem("lease://acme/renderers/a")],
                before: () =>
                {
                    broker.PushNotification(broker.LastSubscriptionId, "lease://acme/renderers/a");
                    return Task.CompletedTask;
                });
        }
        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);

        var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*")
            .WaitAsync(TimeSpan.FromSeconds(1));

        Assert.True(observer.IsReady);
        Assert.InRange(broker.Calls.Count(call => call == "LIST"), 3, 6);
        await observer.DisposeAsync();
    }

    [Fact]
    public async Task ShouldRejectCursorGivenNonProgressingLeaseListWhenBootstrapping()
    {
        var broker = new FakeLeaseBroker();
        var cursor = new LeaseListCursor(7, 10);
        broker.QueueListPage([], cursor);
        broker.QueueListPage([], cursor);
        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);

        var error = await Assert.ThrowsAsync<LeaseException>(() =>
            leaseClient.ObserveAsync("lease://acme/renderers/*"));

        Assert.Equal("LIST_NON_PROGRESSING_CURSOR", error.Code);
        Assert.Equal(2, broker.Calls.Count(call => call == "LIST"));
    }

    [Theory]
    [InlineData(0, 0.2, 256)]
    [InlineData(60, -0.1, 256)]
    [InlineData(60, 1.0, 256)]
    [InlineData(60, 0.2, 0)]
    public async Task ShouldRejectInvalidObserverResourceOptions(
        int intervalSeconds,
        double jitterRatio,
        int updateBufferCapacity)
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);
        var options = new LeaseObserveOptions
        {
            ReconciliationInterval = TimeSpan.FromSeconds(intervalSeconds),
            ReconciliationJitterRatio = jitterRatio,
            UpdateBufferCapacity = updateBufferCapacity,
        };

        // Act
        var act = () => leaseClient.ObserveAsync("lease://acme/renderers/*", options);

        // Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(act);
        Assert.Empty(broker.Calls);
    }

    [Fact]
    public async Task ShouldSubscribeBeforeListingAndApplyBufferedNotificationsAfterFirstListInstalls()
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
    public async Task ShouldRelistOnSteadyStateNotificationsToPreserveCompleteItems()
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
    public async Task ShouldReconcileOnAPeriodicInterval()
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
    public async Task ShouldRebootstrapFromScratchWhenTheConnectionReconnects()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a"), MakeItem("lease://acme/renderers/c") });

        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);

        // Act
        await using var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");
        var subscribeCallsBeforeReconnect = broker.Calls.Count(call => call == "SUBSCRIBE");
        var unsubscribeCallsBeforeReconnect = broker.Calls.Count(call => call == "UNSUBSCRIBE");
        var listCallsBeforeReconnect = broker.Calls.Count(call => call == "LIST");

        await leaseClient.SimulateReconnectAsync();
        await WaitUntilAsync(() => observer.View.ContainsKey("lease://acme/renderers/c"));

        // Assert: RestoreSubscriptionsAsync already re-subscribes the wire-level route, so the
        // observer's own bootstrap must not tear down and re-establish it a second time - exactly
        // one SUBSCRIBE (the restore) and zero UNSUBSCRIBE/extra SUBSCRIBE round trips from the
        // observer, plus exactly one fresh LIST pass.
        Assert.Equal(subscribeCallsBeforeReconnect + 1, broker.Calls.Count(call => call == "SUBSCRIBE"));
        Assert.Equal(unsubscribeCallsBeforeReconnect, broker.Calls.Count(call => call == "UNSUBSCRIBE"));
        Assert.Equal(listCallsBeforeReconnect + 1, broker.Calls.Count(call => call == "LIST"));
        Assert.True(observer.IsReady);
        Assert.True(observer.View.ContainsKey("lease://acme/renderers/c"));
    }

    [Fact]
    public async Task ShouldNotThrowOrLeakWhenDisposedWhileAReconnectBootstrapIsInFlight()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        var listGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") }, before: () => listGate.Task);

        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);

        var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");
        Assert.True(observer.IsReady);

        // Act: a reconnect-triggered bootstrap is blocked mid-LIST while DisposeAsync races it.
        var reconnectTask = leaseClient.SimulateReconnectAsync().AsTask();
        await WaitUntilAsync(() => broker.Calls.Count(call => call == "LIST") == 2);

        var disposeTask = observer.DisposeAsync().AsTask();
        await Task.Delay(50);

        listGate.SetResult();

        var both = Task.WhenAll(reconnectTask, disposeTask);
        var raced = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(both, raced);

        // Assert: neither the reconnect callback nor DisposeAsync throws (no unhandled
        // ObjectDisposedException from a torn-down _refreshGate/_subscription).
        await reconnectTask;
        await disposeTask;

        // No live wire subscription remains after dispose completes.
        Assert.Contains("UNSUBSCRIBE", broker.Calls);

        // No orphaned consumer-loop task keeps running: a post-dispose notification must not
        // resurrect background processing.
        broker.PushNotification(broker.LastSubscriptionId, "lease://acme/renderers/z");
        await Task.Delay(50);
        Assert.False(observer.View.ContainsKey("lease://acme/renderers/z"));
    }

    [Fact]
    public async Task ShouldCoalesceASecondReconnectWhileBootstrapListIsInFlight()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        var listGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/stale") }, before: () => listGate.Task);
        broker.DefaultListPage = new[] { MakeItem("lease://acme/renderers/recovered") };
        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);
        await using var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");

        // Act
        var firstReconnect = leaseClient.SimulateReconnectAsync().AsTask();
        await WaitUntilAsync(() => broker.Calls.Count(call => call == "LIST") == 2);
        var secondReconnect = leaseClient.SimulateReconnectAsync().AsTask();
        listGate.SetResult();
        await Task.WhenAll(firstReconnect, secondReconnect);

        // Assert
        Assert.True(observer.IsReady);
        Assert.True(observer.View.ContainsKey("lease://acme/renderers/recovered"));
        Assert.False(observer.View.ContainsKey("lease://acme/renderers/stale"));
    }

    [Fact]
    public async Task ShouldRetryATransientListFailureAfterReconnect()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        broker.DefaultListPage = new[] { MakeItem("lease://acme/renderers/recovered") };
        using var leaseClient = new LeaseClient(broker.RequestAsync, broker.RegisterNotificationHandler);
        await using var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");
        broker.QueueListFailure(new InvalidOperationException("transient LIST failure"));

        // Act
        await leaseClient.SimulateReconnectAsync();

        // Assert
        await WaitUntilAsync(
            () => observer.IsReady && observer.View.ContainsKey("lease://acme/renderers/recovered"),
            TimeSpan.FromSeconds(5));
        Assert.True(broker.Calls.Count(call => call == "LIST") >= 3);
        Assert.Equal(3, broker.Calls.Count(call => call == "SUBSCRIBE"));
    }

    [Fact]
    public async Task ShouldResubscribeAndRelistAfterDispatchOverflowAndTransientSubscribeFailure()
    {
        // Arrange
        var broker = new FakeLeaseBroker();
        broker.QueueListPage(new[] { MakeItem("lease://acme/renderers/a") });
        var rejectNextDispatch = 1;
        using var leaseClient = new LeaseClient(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(
                await broker.RequestAsync(messageType, payload.ToArray(), ct)),
            (messageType, handler) => broker.RegisterNotificationHandler(
                messageType,
                payload => handler(payload)),
            dispatchAsyncHandler: (_, _) => Interlocked.Exchange(ref rejectNextDispatch, 0) == 0);

        await using var observer = await leaseClient.ObserveAsync("lease://acme/renderers/*");
        Assert.True(observer.IsReady);
        broker.QueueSubscribeFailure(new InvalidOperationException("transient subscribe failure"));
        broker.DefaultListPage = new[] { MakeItem("lease://acme/renderers/recovered") };

        // Act: rejecting dispatch fails and removes the observer's current
        // registration. The first replacement SUBSCRIBE also fails, forcing
        // the recovery loop through its bounded retry path.
        broker.PushNotification(broker.LastSubscriptionId, "lease://acme/renderers/a");

        // Assert
        await WaitUntilAsync(() => !observer.IsReady);
        await WaitUntilAsync(
            () => observer.IsReady && observer.View.ContainsKey("lease://acme/renderers/recovered"),
            TimeSpan.FromSeconds(5));
        Assert.Equal(3, broker.Calls.Count(call => call == "SUBSCRIBE"));
        Assert.True(broker.Calls.Count(call => call == "LIST") >= 2);
    }

    [Fact]
    public async Task ShouldStopBackgroundWorkAndUnsubscribeWhenDisposed()
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

    static LeaseListItem MakeItem(
        string route,
        string owner = "worker-1",
        ulong incarnation = 1,
        string acquiredAt = "2026-08-29T00:00:00Z",
        ulong expiresInSecs = 30,
        uint renewals = 0) => new(route, owner, incarnation, acquiredAt, expiresInSecs, renewals);

    static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
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

    sealed class FakeLeaseBroker
    {
        readonly ConcurrentQueue<(IReadOnlyList<LeaseListItem> Items, LeaseListCursor? Next, Func<Task>? Before)> _listPages = new();
        readonly ConcurrentQueue<Exception> _listFailures = new();
        readonly ConcurrentQueue<Exception> _subscribeFailures = new();
        long _nextSubscriptionId;

        public ConcurrentQueue<string> Calls { get; } = new();

        public ulong LastSubscriptionId { get; private set; }

        public Action<byte[]>? NotifyHandler { get; private set; }

        public IReadOnlyList<LeaseListItem>? DefaultListPage { get; set; }

        public void QueueListPage(IReadOnlyList<LeaseListItem> items, LeaseListCursor? next = null, Func<Task>? before = null) => _listPages.Enqueue((items, next, before));

        public void QueueSubscribeFailure(Exception error) => _subscribeFailures.Enqueue(error);

        public void QueueListFailure(Exception error) => _listFailures.Enqueue(error);

        public async Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken ct)
        {
            if (messageType == MessageTypes.LeaseSubscribe)
            {
                Calls.Enqueue("SUBSCRIBE");
                if (_subscribeFailures.TryDequeue(out var subscribeFailure))
                {
                    throw subscribeFailure;
                }
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
                if (_listFailures.TryDequeue(out var listFailure))
                {
                    throw listFailure;
                }
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
