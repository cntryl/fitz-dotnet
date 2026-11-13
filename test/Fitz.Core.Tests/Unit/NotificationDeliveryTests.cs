using System.Text;
using System.Threading.Tasks.Sources;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class NotificationDeliveryTests
{
    [Fact]
    public async Task ShouldDiscardEarlyNotificationsGivenFailedSubscribeWhenTryingAgain()
    {
        // Arrange
        Action<ReadOnlyMemory<byte>>? notify = null;
        var firstReply = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        using var client = new StreamClient(
            (_, _, _) => ++attempts == 1 ? new ValueTask<ReadOnlyMemory<byte>>(firstReply.Task) : ValueTask.FromResult<ReadOnlyMemory<byte>>(SubscriptionReply()),
            (_, handler) => { notify = handler; return new TestRegistration(); });
        const string route = "stream://prod/app/events";
        var firstSubscribe = client.SubscribeAsync(route);
        notify!(Notification("stream", route));

        // Act
        firstReply.SetException(new ConnectionException("First subscription failed."));
        await Assert.ThrowsAsync<ConnectionException>(() => firstSubscribe);
        var subscription = await client.SubscribeAsync(route);
        notify!(Notification("stream", route, commitOffset: 20));

        // Assert
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await using var reader = subscription.GetAsyncEnumerator(deadline.Token);
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(20UL, reader.Current.CommitOffset);
        Assert.False(subscription.Completion.IsCompleted);
    }

    [Fact]
    public async Task ShouldSkipOldNotificationsGivenReconnectedSessionWhenDispatchPumpResumes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var blocker = mux.RegisterNotificationHandler(MessageTypes.NoticeNotify, _ =>
        {
            blocked.TrySetResult();
            release.Wait(TimeSpan.FromSeconds(10));
        });
        var received = new List<byte>();
        using var registration = mux.RegisterNotificationHandler(MessageTypes.StreamNotify, payload => received.Add(payload[0]));
        mux.Dispatch(MessageTypes.NoticeNotify, new byte[] { 0 });
        await blocked.Task.WaitAsync(TimeSpan.FromSeconds(5));
        mux.Dispatch(MessageTypes.StreamNotify, new byte[] { 1 });

        // Act
        mux.SetDisconnected();
        mux.SetConnected();
        mux.Dispatch(MessageTypes.StreamNotify, new byte[] { 2 });
        release.Set();
        await mux.CompleteNotificationDispatchAsync(TimeSpan.FromSeconds(5), CancellationToken.None);

        // Assert
        Assert.Equal(new byte[] { 2 }, received);
    }

    [Theory]
    [InlineData("stream", false)]
    [InlineData("stream", true)]
    [InlineData("queue", false)]
    [InlineData("queue", true)]
    [InlineData("notice", false)]
    [InlineData("notice", true)]
    [InlineData("kv", false)]
    [InlineData("kv", true)]
    [InlineData("schedule", false)]
    [InlineData("schedule", true)]
    [InlineData("lease", false)]
    [InlineData("lease", true)]
    public async Task ShouldResolveDeliveryGivenSuccessfulReplyBeforeActivationWhenSubscribing(string domain, bool overflow)
    {
        // Arrange
        using var response = new PausedResponse();
        Action<ReadOnlyMemory<byte>>? notify = null;
        ValueTask<ReadOnlyMemory<byte>> Request(ushort type, ReadOnlyMemory<byte> payload, CancellationToken ct) => response.Task;
        IDisposable Register(ushort type, Action<ReadOnlyMemory<byte>> handler)
        {
            notify = handler;
            return new TestRegistration();
        }
        using IDisposable client = domain switch
        {
            "stream" => new StreamClient(Request, Register, subscriptionBufferCapacity: 1),
            "queue" => new QueueClient(Request, Register, subscriptionBufferCapacity: 1),
            "notice" => new NoticeClient((_, _, _) => ValueTask.CompletedTask, Request, Register, subscriptionBufferCapacity: 1),
            "kv" => new KvClient(Request, registerNotificationHandler: Register, subscriptionBufferCapacity: 1),
            "schedule" => new ScheduleClient(Request, Register, subscriptionBufferCapacity: 1),
            "lease" => new LeaseClient(Request, registerNotificationHandler: Register, subscriptionBufferCapacity: 1),
            _ => throw new ArgumentOutOfRangeException(nameof(domain)),
        };
        var route = $"{domain}://prod/app/events" + (domain == "schedule" ? "/run" : string.Empty);
        async Task<SubscriptionHandle> SubscribeAsync() => client switch
        {
            StreamClient value => await value.SubscribeAsync(route),
            QueueClient value => await value.SubscribeAsync(route),
            NoticeClient value => await value.SubscribeAsync(route),
            KvClient value => await value.SubscribeAsync(route),
            ScheduleClient value => await value.SubscribeAsync(route),
            LeaseClient value => await value.SubscribeAsync(route),
            _ => throw new InvalidOperationException(),
        };
        var subscribing = SubscribeAsync();

        // Act
        response.Complete(SubscriptionReply(domain));
        await response.Resuming.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try
        {
            notify!(Notification(domain, route));
            if (overflow)
                notify!(Notification(domain, route));
        }
        finally
        {
            response.AllowResume.Set();
        }
        var subscription = await subscribing.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        if (overflow)
        {
            await Assert.ThrowsAsync<SubscriptionBackpressureException>(() => subscription.Completion.WaitAsync(TimeSpan.FromSeconds(2)));
            return;
        }
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var receivedRoute = subscription switch
        {
            StreamSubscription value => await FirstRouteAsync(value, evt => evt.Route, deadline.Token),
            QueueSubscription value => await FirstRouteAsync(value, evt => evt.Route, deadline.Token),
            NoticeSubscription value => await FirstRouteAsync(value, evt => evt.Route, deadline.Token),
            KvSubscription value => await FirstRouteAsync(value, evt => evt.Route, deadline.Token),
            ScheduleSubscription value => await FirstRouteAsync(value, evt => evt.Route, deadline.Token),
            LeaseSubscription value => await FirstRouteAsync(value, evt => evt.Route, deadline.Token),
            _ => throw new InvalidOperationException(),
        };
        Assert.Equal(route, receivedRoute);
        Assert.False(subscription.Completion.IsCompleted);
    }

    [Fact]
    public async Task ShouldDeliverCountersGivenBrokerQueueNotificationWhenSubscribed()
    {
        // Arrange
        Action<ReadOnlyMemory<byte>>? notify = null;
        using var client = new QueueClient(
            (_, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(SubscriptionReply()),
            (_, handler) => { notify = handler; return new TestRegistration(); });
        var subscription = await client.SubscribeAsync("queue://prod/app/events");

        // Act
        notify!(Notification("queue", "queue://prod/app/events"));

        // Assert
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await using var reader = subscription.GetAsyncEnumerator(deadline.Token);
        Assert.True(await reader.MoveNextAsync());
        var payload = new BinaryBufferReader(reader.Current.Payload);
        Assert.Equal(1UL, payload.ReadU64());
        Assert.Equal(2UL, payload.ReadU64());
        Assert.Equal(3UL, payload.ReadU64());
        Assert.True(payload.IsEof);
    }

    [Theory]
    [InlineData("stream", MessageTypes.StreamSubscribe)]
    [InlineData("queue", MessageTypes.QueueSubscribe)]
    [InlineData("notice", MessageTypes.NoticeSubscribe)]
    [InlineData("kv", MessageTypes.KvSubscribe)]
    [InlineData("schedule", MessageTypes.ScheduleSubscribe)]
    [InlineData("lease", MessageTypes.LeaseSubscribe)]
    public async Task ShouldFaultSubscriptionGivenSharedNotificationOverflowWhenPumpIsBlocked(string domain, ushort subscribeType)
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        await using var connection = new FitzConnection(new ClientConfig(
            new Uri("ws://queued/ws"), Reconnect: new ReconnectOptions(Enabled: false)), () => transport);
        using IDisposable client = domain switch
        {
            "stream" => new StreamClient(connection),
            "queue" => new QueueClient(connection),
            "notice" => new NoticeClient(connection),
            "kv" => new KvClient(connection),
            "schedule" => new ScheduleClient(connection),
            "lease" => new LeaseClient(connection),
            _ => throw new ArgumentOutOfRangeException(nameof(domain)),
        };
        await connection.ConnectAsync();
        transport.QueueResponse(subscribeType, SubscriptionReply(domain));
        var route = $"{domain}://prod/app/events" + (domain == "schedule" ? "/run" : string.Empty);
        SubscriptionHandle subscription = client switch
        {
            StreamClient value => await value.SubscribeAsync(route),
            QueueClient value => await value.SubscribeAsync(route),
            NoticeClient value => await value.SubscribeAsync(route),
            KvClient value => await value.SubscribeAsync(route),
            ScheduleClient value => await value.SubscribeAsync(route),
            LeaseClient value => await value.SubscribeAsync(route),
            _ => throw new InvalidOperationException(),
        };
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var blocker = connection.RegisterNotificationHandler(MessageTypes.NoticeNotify, _ =>
        {
            blocked.TrySetResult();
            release.Wait(TimeSpan.FromSeconds(10));
        });
        transport.QueueResponse(MessageTypes.NoticeNotify, new byte[] { 0 });
        await blocked.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Act
        for (var i = 0; i < 1025; i++)
            transport.QueueResponse(MessageTypes.NoticeNotify, new byte[] { 0 });

        // Assert
        try
        {
            await Assert.ThrowsAsync<SubscriptionBackpressureException>(() => subscription.Completion.WaitAsync(TimeSpan.FromSeconds(3)));
        }
        finally
        {
            release.Set();
        }
    }

    static async Task<string> FirstRouteAsync<T>(SubscriptionHandle<T> subscription, Func<T, string> route, CancellationToken ct)
    {
        await using var reader = subscription.GetAsyncEnumerator(ct);
        Assert.True(await reader.MoveNextAsync());
        return route(reader.Current);
    }

    static byte[] SubscriptionReply(string domain = "stream")
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        if (domain is not ("kv" or "lease"))
            writer.WriteU8(1);
        writer.WriteU64(55);
        return writer.Build();
    }

    static byte[] Notification(string domain, string route, ulong commitOffset = 19)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(55);
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
            var payload = domain == "stream" ? Encoding.UTF8.GetBytes($"{{\"last_resource_offset\":{commitOffset}}}") : Array.Empty<byte>();
            writer.WriteU32((uint)payload.Length);
            writer.WriteBytes(payload);
        }
        return writer.Build();
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
