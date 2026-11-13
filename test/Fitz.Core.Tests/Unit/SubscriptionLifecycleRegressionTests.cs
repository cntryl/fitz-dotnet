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
    [Fact]
    public async Task ShouldRejectQueuedSubscribeGivenDisposedDomainWhenSubscriptionGateBecomesAvailable()
    {
        // Arrange
        var firstReply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wireRequests = 0;
        using var fixture = new DomainFixture("stream", (_, _, _) =>
            ++wireRequests == 1 ? new ValueTask<ReadOnlyMemory<byte>>(firstReply.Task)
                : ValueTask.FromResult<ReadOnlyMemory<byte>>(Reply("stream", 56)));
        var first = fixture.SubscribeAsync(Route("stream", "first"));
        var queued = fixture.SubscribeAsync(Route("stream", "queued"));

        // Act
        fixture.Dispose();
        firstReply.SetResult(Reply("stream", 55));

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
        using var fixture = new DomainFixture(domain, (type, _, _) =>
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
        using var fixture = new DomainFixture(domain, (_, _, _) => response.Task);
        var subscribing = fixture.SubscribeAsync(Route(domain, "events"));
        response.Complete(Reply(domain, 55));
        await response.Resuming.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Act
        try
        {
            fixture.Dispose();
        }
        finally
        {
            response.AllowResume.Set();
        }

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

    sealed class DomainFixture : IDisposable
    {
        readonly IDisposable _client;
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

        internal async Task<SubscriptionHandle> SubscribeAsync(string route) => _client switch
        {
            StreamClient value => await value.SubscribeAsync(route),
            QueueClient value => await value.SubscribeAsync(route),
            NoticeClient value => await value.SubscribeAsync(route),
            KvClient value => await value.SubscribeAsync(route),
            ScheduleClient value => await value.SubscribeAsync(route),
            LeaseClient value => await value.SubscribeAsync(route),
            _ => throw new InvalidOperationException(),
        };

        internal void Notify(byte[] payload) => _notify!(payload);
        public void Dispose() => _client.Dispose();
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
