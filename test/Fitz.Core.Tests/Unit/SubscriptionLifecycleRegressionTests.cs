using System.Text;
using System.Threading.Tasks.Sources;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SubscriptionLifecycleRegressionTests
{
    static readonly TimeSpan Deadline = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ShouldEndOwningSessionGivenSentSubscribeWhenAcknowledgementTimesOut()
    {
        // Arrange
        var time = new ManualTimeProvider();
        var timeout = TimeSpan.FromSeconds(19);
        var lost = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var transport = new TestQueuedTransport();
        var config = new ClientConfig(transport.Url)
        {
            Timeout = timeout,
            TransportFactory = _ => transport,
            Reconnect = new ReconnectOptions(Enabled: false),
            Observability = new FitzObservabilityOptions(OnLifecycleEvent: evt =>
            {
                if (evt.Event == "connection_lost")
                    lost.TrySetResult();
            }),
        };
        await using var client = new FitzClient(config, time);
        await client.ConnectAsync().WaitAsync(Deadline);
        var pending = client.Stream.SubscribeAsync(Route("stream", "events"));

        // Act
        await time.WaitForTimerAsync(timeout);
        time.Advance(timeout);

        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(() => pending.WaitAsync(Deadline));
        await lost.Task.WaitAsync(Deadline);
    }

    [Fact]
    public async Task ShouldFreeAdmissionGivenCanceledQueuedSelectorsWhenAnotherSelectorArrives()
    {
        // Arrange
        var firstReply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var submits = 0;
        await using var fixture = new DomainFixture("notice", (type, _, _) =>
        {
            if (type != MessageTypes.NoticeSubscribe)
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0 });
            return Interlocked.Increment(ref submits) == 1
                ? new ValueTask<ReadOnlyMemory<byte>>(firstReply.Task)
                : ValueTask.FromResult<ReadOnlyMemory<byte>>(Reply("notice", 56));
        });
        var first = fixture.SubscribeAsync(Route("notice", "first"));
        using var stop = new CancellationTokenSource();
        var pending = Task.WhenAll(Enumerable.Range(0, ClientConfig.DefaultRequestQueueSize)
            .Select(i => fixture.SubscribeAsync(Route("notice", $"queued{i}"), stop.Token)));
        Task<SubscriptionHandle>? last = null;

        // Act
        try
        {
            await stop.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(Deadline));
            last = fixture.SubscribeAsync(Route("notice", "last"));
            Assert.False(last.IsCompleted);
        }
        finally
        {
            firstReply.SetResult(Reply("notice", 55));
        }

        // Assert
        await using var initial = await first.WaitAsync(Deadline);
        await using var admitted = await last.WaitAsync(Deadline);
        Assert.Equal(2, submits);
    }

    [Fact]
    public async Task ShouldKeepRemovalOrderedGivenWithdrawWaitCanceledWhenSameSelectorIsRequested()
    {
        // Arrange
        var reply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var submits = 0;
        await using var fixture = new DomainFixture("stream", (type, _, token) => type == MessageTypes.StreamSubscribe
            ? ValueTask.FromResult<ReadOnlyMemory<byte>>(Reply("stream", (ulong)Interlocked.Increment(ref submits)))
            : new ValueTask<ReadOnlyMemory<byte>>(reply.Task.WaitAsync(token)));
        var route = Route("stream", "events");
        await using var first = await fixture.SubscribeAsync(route);
        using var ct = new CancellationTokenSource();
        var withdrawing = first.UnsubscribeAsync(ct.Token).AsTask();
        Task<SubscriptionHandle>? next = null;

        // Act
        try
        {
            await ct.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => withdrawing.WaitAsync(Deadline));
            next = fixture.SubscribeAsync(route);
            Assert.False(next.IsCompleted);
        }
        finally
        {
            reply.SetResult(new byte[] { 0 });
        }

        // Assert
        await using var replacement = await next.WaitAsync(Deadline);
        await first.Completion.WaitAsync(Deadline);
        Assert.Equal(2, submits);
    }

    [Fact]
    public async Task ShouldKeepSharedActivationGivenFirstWaiterCancelsWhenAnotherWaiterRemains()
    {
        // Arrange
        var reply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscribes = 0;
        var unsubscribes = 0;
        await using var fixture = new DomainFixture("stream", (type, _, _) =>
        {
            if (type == MessageTypes.StreamSubscribe)
            {
                Interlocked.Increment(ref subscribes);
                return new ValueTask<ReadOnlyMemory<byte>>(reply.Task);
            }
            Interlocked.Increment(ref unsubscribes);
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0 });
        });
        var route = Route("stream", "events");
        using var ct = new CancellationTokenSource();
        var first = fixture.SubscribeAsync(route, ct.Token);
        var sibling = fixture.SubscribeAsync(route);

        // Act
        try
        {
            await ct.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(Deadline));
            fixture.Notify(Notification("stream", 55, route));
        }
        finally
        {
            reply.SetResult(Reply("stream", 55));
        }

        // Assert
        await using var surviving = await sibling.WaitAsync(Deadline);
        using var deadline = new CancellationTokenSource(Deadline);
        Assert.Equal(route, await FirstRouteAsync(surviving, deadline.Token));
        await surviving.UnsubscribeAsync(deadline.Token);
        Assert.Equal(1, subscribes);
        Assert.Equal(1, unsubscribes);
    }

    [Fact]
    public async Task ShouldWaitForWithdrawalGivenCanceledOnlyWaiterWhenSelectorIsRequestedAgain()
    {
        // Arrange
        var reply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var remove = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var removing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscribes = 0;
        await using var fixture = new DomainFixture("stream", (type, _, _) =>
        {
            if (type == MessageTypes.StreamSubscribe)
                return Interlocked.Increment(ref subscribes) == 1
                    ? new ValueTask<ReadOnlyMemory<byte>>(reply.Task)
                    : ValueTask.FromResult<ReadOnlyMemory<byte>>(Reply("stream", 56));
            removing.TrySetResult();
            return new ValueTask<ReadOnlyMemory<byte>>(remove.Task);
        });
        var route = Route("stream", "events");
        using var ct = new CancellationTokenSource();
        var canceled = fixture.SubscribeAsync(route, ct.Token);
        Task<SubscriptionHandle>? next = null;

        // Act
        try
        {
            await ct.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled.WaitAsync(Deadline));
            reply.SetResult(Reply("stream", 55));
            await removing.Task.WaitAsync(Deadline);
            next = fixture.SubscribeAsync(route);
            Assert.False(next.IsCompleted);
            Assert.Equal(1, subscribes);
        }
        finally
        {
            reply.TrySetResult(Reply("stream", 55));
            remove.SetResult(new byte[] { 0 });
        }

        // Assert
        await using var subscription = await next.WaitAsync(Deadline);
        using var deadline = new CancellationTokenSource(Deadline);
        fixture.Notify(Notification("stream", 56, route));
        Assert.Equal(route, await FirstRouteAsync(subscription, deadline.Token));
        Assert.Equal(2, subscribes);
    }

    [Fact]
    public async Task ShouldDeliverToFastReaderGivenSharedSelectorWhenSiblingOverflows()
    {
        // Arrange
        var unsubscribes = 0;
        await using var fixture = new DomainFixture("stream", (type, _, _) =>
        {
            if (type == MessageTypes.StreamSubscribe)
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(Reply("stream", 55));
            Interlocked.Increment(ref unsubscribes);
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0 });
        });
        var route = Route("stream", "events");
        await using var slow = await fixture.SubscribeAsync(route);
        await using var fast = (StreamSubscription)await fixture.SubscribeAsync(route);
        using var deadline = new CancellationTokenSource(Deadline);
        await using var reader = fast.GetAsyncEnumerator(deadline.Token);

        // Act
        fixture.Notify(Notification("stream", 55, route));
        Assert.True(await reader.MoveNextAsync());
        fixture.Notify(Notification("stream", 55, route));

        // Assert
        await Assert.ThrowsAsync<SubscriptionBackpressureException>(() => slow.Completion.WaitAsync(Deadline));
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(route, reader.Current.Route);
        Assert.False(fast.Completion.IsCompleted);
        Assert.Equal(0, unsubscribes);
    }

    [Fact]
    public async Task ShouldActivateIndependentRoutesGivenOverlappingSelectorsWhenRepliesArriveOutOfOrder()
    {
        // Arrange
        var exactReply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wildcardReply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var exactRoute = Route("stream", "events");
        var wildcard = Route("stream", "*");
        await using var fixture = new DomainFixture("stream", (type, payload, _) => type == MessageTypes.StreamSubscribe
            ? new ValueTask<ReadOnlyMemory<byte>>(new BinaryBufferReader(payload).ReadString() == exactRoute ? exactReply.Task : wildcardReply.Task)
            : ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0 }));
        var exact = fixture.SubscribeAsync(exactRoute);
        var area = fixture.SubscribeAsync(wildcard);

        // Act
        fixture.Notify(Notification("stream", 55, exactRoute));
        fixture.Notify(Notification("stream", 56, exactRoute));
        wildcardReply.SetResult(Reply("stream", 56));
        try
        {
            await using var wildcardHandle = await area.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.False(exact.IsCompleted);
            exactReply.SetResult(Reply("stream", 55));
            await using var exactHandle = await exact.WaitAsync(TimeSpan.FromSeconds(2));

            // Assert
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            Assert.Equal(exactRoute, await FirstRouteAsync(wildcardHandle, deadline.Token));
            Assert.Equal(exactRoute, await FirstRouteAsync(exactHandle, deadline.Token));
        }
        finally
        {
            exactReply.TrySetResult(Reply("stream", 55));
        }
    }

    [Fact]
    public async Task ShouldRejectQueuedSubscribeGivenDisposedDomainWhenSubscriptionGateBecomesAvailable()
    {
        // Arrange
        var firstReply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wireRequests = 0;
        await using var fixture = new DomainFixture("notice", (_, _, _) =>
            ++wireRequests == 1 ? new ValueTask<ReadOnlyMemory<byte>>(firstReply.Task)
                : ValueTask.FromResult<ReadOnlyMemory<byte>>(Reply("notice", 56)));
        var first = fixture.SubscribeAsync(Route("notice", "first"));
        var queued = fixture.SubscribeAsync(Route("notice", "queued"));

        // Act: disposal waits for the in-flight subscribe, so release it after disposal starts.
        var disposing = fixture.DisposeAsync().AsTask();
        firstReply.SetResult(Reply("notice", 55));
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => first.WaitAsync(TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => queued.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(1, wireRequests);
    }

    [Theory]
    [InlineData("stream", MessageTypes.StreamSubscribe, false)]
    [InlineData("stream", MessageTypes.StreamSubscribe, true)]
    [InlineData("queue", MessageTypes.QueueSubscribe, false)]
    [InlineData("queue", MessageTypes.QueueSubscribe, true)]
    [InlineData("notice", MessageTypes.NoticeSubscribe, false)]
    [InlineData("notice", MessageTypes.NoticeSubscribe, true)]
    [InlineData("kv", MessageTypes.KvSubscribe, false)]
    [InlineData("kv", MessageTypes.KvSubscribe, true)]
    [InlineData("schedule", MessageTypes.ScheduleSubscribe, false)]
    [InlineData("schedule", MessageTypes.ScheduleSubscribe, true)]
    [InlineData("lease", MessageTypes.LeaseSubscribe, false)]
    [InlineData("lease", MessageTypes.LeaseSubscribe, true)]
    public async Task ShouldIgnoreRetiredSubscriptionBacklogGivenPendingSubscriptionWhenActivating(string domain, ushort subscribeType, bool reusePattern)
    {
        // Arrange
        var secondReply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscribes = 0;
        await using var fixture = new DomainFixture(domain, (type, _, _) =>
            type != subscribeType ? ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 0 })
                : ++subscribes == 1 ? ValueTask.FromResult<ReadOnlyMemory<byte>>(Reply(domain, 55))
                : new ValueTask<ReadOnlyMemory<byte>>(secondReply.Task));
        var oldRoute = Route(domain, "old");
        var newRoute = reusePattern ? oldRoute : Route(domain, "new");
        var old = await fixture.SubscribeAsync(oldRoute);
        await old.UnsubscribeAsync();
        var subscribing = fixture.SubscribeAsync(newRoute);

        // Act
        fixture.Notify(Notification(domain, 55, oldRoute));
        fixture.Notify(Notification(domain, 55, oldRoute));
        secondReply.SetResult(Reply(domain, 56));
        var subscription = await subscribing.WaitAsync(TimeSpan.FromSeconds(5));
        fixture.Notify(Notification(domain, 56, newRoute));

        // Assert
        Assert.False(subscription.Completion.IsCompleted);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        Assert.Equal(newRoute, await FirstRouteAsync(subscription, deadline.Token));
    }

    [Theory]
    [InlineData("stream")]
    [InlineData("queue")]
    [InlineData("notice")]
    [InlineData("kv")]
    [InlineData("schedule")]
    [InlineData("lease")]
    public async Task ShouldRejectSubscriptionGivenDomainDisposedBeforeSuccessfulReplyResumesWhenSubscribing(string domain)
    {
        // Arrange
        using var response = new PausedResponse();
        await using var fixture = new DomainFixture(domain, (_, _, _) => response.Task);
        var subscribing = fixture.SubscribeAsync(Route(domain, "events"));
        response.Complete(Reply(domain, 55));
        await response.Resuming.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Act: disposal waits for the in-flight subscribe, so release it after disposal starts.
        var disposing = fixture.DisposeAsync().AsTask();
        response.AllowResume.Set();
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => subscribing.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task ShouldAllowSequentialEnumerationGivenEnumeratorDisposedBeforeFirstMoveWhenEnumeratingAgain()
    {
        // Arrange
        await using var subscription = new StreamSubscription("stream://r/a/x", _ => ValueTask.CompletedTask);
        var first = subscription.GetAsyncEnumerator();

        // Act
        await first.DisposeAsync();

        // Assert
        await using var second = subscription.GetAsyncEnumerator();
        Assert.False(await second.MoveNextAsync());
    }

    [Fact]
    public async Task ShouldRejectConcurrentEnumerationGivenUnstartedLiveEnumeratorWhenEnumeratingAgain()
    {
        // Arrange
        await using var subscription = new StreamSubscription("stream://r/a/x", _ => ValueTask.CompletedTask);
        await using var first = subscription.GetAsyncEnumerator();

        // Act
        var error = Assert.Throws<InvalidOperationException>(() => subscription.GetAsyncEnumerator());

        // Assert
        Assert.Contains("Concurrent enumeration", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldPreserveNewEnumerationGuardGivenCompletedOldEnumeratorWhenOldEnumeratorIsDisposed()
    {
        // Arrange
        await using var subscription = new StreamSubscription("stream://r/a/x", _ => ValueTask.CompletedTask);
        var first = subscription.GetAsyncEnumerator();
        Assert.False(await first.MoveNextAsync());
        await using var second = subscription.GetAsyncEnumerator();

        // Act
        await first.DisposeAsync();

        // Assert
        Assert.Throws<InvalidOperationException>(() => subscription.GetAsyncEnumerator());
    }

    static string Route(string domain, string resource) => $"{domain}://prod/app/{resource}" + (domain == "schedule" ? "/run" : string.Empty);

    static async Task<string> FirstRouteAsync(SubscriptionHandle subscription, CancellationToken ct) => subscription switch
    {
        StreamSubscription value => await FirstRouteAsync(value, evt => evt.Route, ct),
        QueueSubscription value => await FirstRouteAsync(value, evt => evt.Route, ct),
        NoticeSubscription value => await FirstRouteAsync(value, evt => evt.Route, ct),
        KvSubscription value => await FirstRouteAsync(value, evt => evt.Route, ct),
        ScheduleSubscription value => await FirstRouteAsync(value, evt => evt.Route, ct),
        LeaseSubscription value => await FirstRouteAsync(value, evt => evt.Route, ct),
        _ => throw new InvalidOperationException(),
    };

    static async Task<string> FirstRouteAsync<T>(SubscriptionHandle<T> subscription, Func<T, string> route, CancellationToken ct)
    {
        await using var reader = subscription.GetAsyncEnumerator(ct);
        Assert.True(await reader.MoveNextAsync());
        return route(reader.Current);
    }

    static byte[] Reply(string domain, ulong id)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        if (domain is not ("kv" or "lease"))
            writer.WriteU8(1);
        writer.WriteU64(id);
        return writer.Build();
    }

    static byte[] Notification(string domain, ulong id, string route)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(id);
        writer.WriteString(route);
        if (domain == "queue")
        {
            writer.WriteU64(1);
            writer.WriteU64(2);
            writer.WriteU64(3);
        }
        else if (domain == "kv")
        {
            writer.WriteU64(1);
        }
        else
        {
            var payload = domain == "stream" ? Encoding.UTF8.GetBytes("{\"last_resource_offset\":19}") : Array.Empty<byte>();
            writer.WriteU32((uint)payload.Length);
            writer.WriteBytes(payload);
        }
        return writer.Build();
    }

    sealed class DomainFixture : IAsyncDisposable
    {
        readonly IAsyncDisposable _client;
        Action<ReadOnlyMemory<byte>>? _notify;

        internal DomainFixture(string domain, Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request)
        {
            IDisposable Register(ushort type, Action<ReadOnlyMemory<byte>> handler)
            {
                _notify = handler;
                return new TestRegistration();
            }
            _client = domain switch
            {
                "stream" => new StreamClient(request, Register, subscriptionBufferCapacity: 1),
                "queue" => new QueueClient(request, Register, subscriptionBufferCapacity: 1),
                "notice" => new NoticeClient((_, _, _) => ValueTask.CompletedTask, request, Register, subscriptionBufferCapacity: 1),
                "kv" => new KvClient(request, registerNotificationHandler: Register, subscriptionBufferCapacity: 1),
                "schedule" => new ScheduleClient(request, Register, subscriptionBufferCapacity: 1),
                "lease" => new LeaseClient(request, registerNotificationHandler: Register, subscriptionBufferCapacity: 1),
                _ => throw new ArgumentOutOfRangeException(nameof(domain)),
            };
        }

        internal async Task<SubscriptionHandle> SubscribeAsync(string route, CancellationToken ct = default) => _client switch
        {
            StreamClient value => await value.SubscribeAsync(route, ct),
            QueueClient value => await value.SubscribeAsync(route, ct),
            NoticeClient value => await value.SubscribeAsync(route, ct),
            KvClient value => await value.SubscribeAsync(route, ct),
            ScheduleClient value => await value.SubscribeAsync(route, ct),
            LeaseClient value => await value.SubscribeAsync(route, ct),
            _ => throw new InvalidOperationException(),
        };

        internal void Notify(byte[] payload) => _notify!(payload);
        public ValueTask DisposeAsync() => _client.DisposeAsync();
    }

    sealed class PausedResponse : IValueTaskSource<ReadOnlyMemory<byte>>, IDisposable
    {
        ManualResetValueTaskSourceCore<ReadOnlyMemory<byte>> _source = new() { RunContinuationsAsynchronously = true };
        internal TaskCompletionSource Resuming { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal ManualResetEventSlim AllowResume { get; } = new();
        internal ValueTask<ReadOnlyMemory<byte>> Task => new(this, _source.Version);
        internal void Complete(byte[] response) => _source.SetResult(response);
        public ReadOnlyMemory<byte> GetResult(short token)
        {
            var result = _source.GetResult(token);
            Resuming.TrySetResult();
            if (!AllowResume.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Subscription continuation was not released.");
            return result;
        }
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
        public void Dispose() => AllowResume.Dispose();
    }
}
