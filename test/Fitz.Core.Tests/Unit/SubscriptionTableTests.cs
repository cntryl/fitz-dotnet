using System.Text;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SubscriptionTableTests
{
    [Fact]
    public async Task ShouldRecoverGivenOverflowedRestoreWhenReplacementSessionPublishesNotifications()
    {
        // Arrange
        const ushort notificationType = 609;
        const string pattern = "stream://r/a/x";
        await using var original = new TestQueuedTransport();
        await using var overflowing = new TestQueuedTransport();
        await using var replacement = new TestQueuedTransport();
        TestQueuedTransport[] transports = [original, overflowing, replacement];
        var connects = 0;
        var config = new ClientConfig(original.Url)
        {
            Reconnect = new ReconnectOptions(Backoff: TimeSpan.FromMilliseconds(1)),
        };
        await using var connection = new FitzConnection(config,
            () => transports[Interlocked.Increment(ref connects) - 1]);
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var table = new SubscriptionTable<string>(
            this,
            "stream",
            notificationType,
            registerNotificationHandler: null,
            dispatchAsyncHandler: null,
            bufferCapacity: 8,
            (_, _, _) => Task.FromResult(1UL),
            (_, _, _, _) => Task.CompletedTask,
            payload => new DecodedNotification<string>(1, pattern, Encoding.UTF8.GetString(payload.Span))
        );
        using var binding = table.AttachTo(connection);
        using var restore = connection.OnReconnect(async ct =>
        {
            var failedAttempt = Volatile.Read(ref connects) == 2;
            var transport = failedAttempt ? overflowing : replacement;
            var frame = FrameCodec.Encode(notificationType, failedAttempt ? "overflow"u8 : "replacement"u8);
            var count = failedAttempt ? Multiplexer.NotificationQueueCapacity + 1 : 1;

            for (var i = 0; i < count; i++)
                transport.QueueIncomingFrame(frame);

            // A response behind those frames confirms the receive loop has staged all of them.
            transport.QueueResponse(MessageTypes.StreamGetMetadata, new byte[] { 0 });
            _ = await connection.RequestOnSessionAsync(MessageTypes.StreamGetMetadata,
                ReadOnlyMemory<byte>.Empty, connection.ConnectionClosedToken, ct);
        });
        await connection.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(5));
        var handle = await table.SubscribeAsync(pattern, (value, _) =>
        {
            delivered.TrySetResult(value);
            return ValueTask.CompletedTask;
        }, preDispatch: null, CancellationToken.None);

        // Act
        original.QueueClosed();

        // Assert
        Assert.Equal("replacement", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        _ = await connection.WaitUntilReadyAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(3, connects);
        Assert.False(handle.Completion.IsCompleted);
    }

    [Fact]
    public async Task ShouldFenceOldDeliveryGivenReusedSubscriptionIdWhenNotificationDecodingSpansReconnect()
    {
        // Arrange
        const ushort notificationType = 609;
        const string pattern = "stream://r/a/x";
        await using var original = new TestQueuedTransport();
        await using var replacement = new TestQueuedTransport();
        var connects = 0;
        var config = new ClientConfig(original.Url)
        {
            Reconnect = new ReconnectOptions(Backoff: TimeSpan.FromMilliseconds(1)),
        };
        await using var connection = new FitzConnection(config,
            () => Interlocked.Increment(ref connects) == 1 ? original : replacement);
        using var unblock = new ManualResetEventSlim();
        var decoding = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var table = new SubscriptionTable<string>(
            this,
            "stream",
            notificationType,
            registerNotificationHandler: null,
            dispatchAsyncHandler: null,
            bufferCapacity: 8,
            (_, _, _) => Task.FromResult(1UL),
            (_, _, _, _) => Task.CompletedTask,
            payload =>
            {
                var value = Encoding.UTF8.GetString(payload.Span);

                if (value == "old")
                {
                    decoding.TrySetResult();
                    _ = unblock.Wait(TimeSpan.FromSeconds(10));
                }

                return new DecodedNotification<string>(1, pattern, value);
            }
        );
        using var binding = table.AttachTo(connection);
        await connection.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(5));
        _ = await table.SubscribeAsync(pattern, (value, _) =>
        {
            delivered.TrySetResult(value);
            return ValueTask.CompletedTask;
        }, preDispatch: null, CancellationToken.None);
        original.QueueIncomingFrame(FrameCodec.Encode(notificationType, "old"u8));
        await decoding.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Act
        try
        {
            var originalSession = connection.ConnectionClosedToken;
            var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var closed = originalSession.Register(() => disconnected.TrySetResult());
            original.QueueClosed();
            await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
            _ = await connection.WaitUntilReadyAsync(TimeSpan.FromSeconds(5));
            replacement.QueueIncomingFrame(FrameCodec.Encode(notificationType, "replacement"u8));
        }
        finally
        {
            unblock.Set();
        }

        // Assert
        Assert.Equal("replacement", await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }
    [Fact]
    public async Task ShouldWithdrawRestoredSubscriptionGivenFollowCleanupDuringRestore()
    {
        // Arrange
        var restoring = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var fake = new FakeWire { RestoreReply = restoring.Task };
        await using var table = fake.CreateTable();
        var follow = await table.SubscribeAsync("kv://a/b/*", Ignore, preDispatch: null, CancellationToken.None, bestEffort: true);
        var restore = table.RestoreAsync(CancellationToken.None).AsTask();
        Assert.Equal(2, fake.SubscribeCalls);

        // Act
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => follow.Unsubscribe(new CancellationToken(true)).AsTask());
        restoring.SetResult(2);
        await restore;

        // Assert
        Assert.Equal([("kv://a/b/*", 2UL)], fake.Unsubscribed);
    }

    [Fact]
    public async Task ShouldUnsubscribeRestoredPatternsGivenPartialRestoreFailureWhenReconnecting()
    {
        // Arrange
        var fake = new FakeWire { FailSubscribeCall = 4 };
        await using var table = fake.CreateTable();
        _ = await table.SubscribeAsync("kv://a/b/*", Ignore, preDispatch: null, CancellationToken.None);
        _ = await table.SubscribeAsync("kv://a/c/*", Ignore, preDispatch: null, CancellationToken.None);

        // Act
        var act = () => table.RestoreAsync(CancellationToken.None).AsTask();

        // Assert
        await Assert.ThrowsAsync<InvalidOperationException>(act);
        Assert.Equal([("kv://a/b/*", 3UL)], fake.Unsubscribed);
    }

    [Fact]
    public async Task ShouldRouteNotificationsByNewIdGivenSuccessfulRestoreWhenReconnecting()
    {
        // Arrange
        var fake = new FakeWire();
        await using var table = fake.CreateTable();
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = await table.SubscribeAsync(
            "kv://a/b/*",
            (value, _) =>
            {
                received.TrySetResult(value);
                return ValueTask.CompletedTask;
            },
            preDispatch: null,
            CancellationToken.None);

        // Act
        await table.RestoreAsync(CancellationToken.None);
        fake.Deliver(subscriptionId: 1, "stale");
        fake.Deliver(subscriptionId: 2, "fresh");

        // Assert
        Assert.Equal("fresh", await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task ShouldShareOneBrokerSubscriptionGivenSiblingHandlesWhenLastLeaves()
    {
        // Arrange
        var fake = new FakeWire();
        await using var table = fake.CreateTable();
        var first = await table.SubscribeAsync("kv://a/b/*", Ignore, preDispatch: null, CancellationToken.None);
        var second = await table.SubscribeAsync("kv://a/b/*", Ignore, preDispatch: null, CancellationToken.None);

        // Act
        await first.Unsubscribe(CancellationToken.None);
        var afterFirst = fake.Unsubscribed.Count;
        await second.Unsubscribe(CancellationToken.None);

        // Assert
        Assert.Equal(1, fake.SubscribeCalls);
        Assert.Equal(0, afterFirst);
        Assert.Equal([("kv://a/b/*", 1UL)], fake.Unsubscribed);
    }

    [Fact]
    public async Task ShouldFaultOnlyThatHandleGivenPreDispatchThrowsWhenNotified()
    {
        // Arrange
        var fake = new FakeWire();
        await using var table = fake.CreateTable();
        var rejecting = await table.SubscribeAsync(
            "kv://a/b/*",
            Ignore,
            _ => throw new FitzException("rejected"),
            CancellationToken.None);
        var accepting = await table.SubscribeAsync("kv://a/b/*", Ignore, preDispatch: null, CancellationToken.None);

        // Act
        fake.Deliver(subscriptionId: 1, "value");

        // Assert
        var error = await Assert.ThrowsAsync<FitzException>(() => rejecting.Completion.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("rejected", error.Message);
        Assert.False(accepting.Completion.IsCompleted);
    }

    [Fact]
    public async Task ShouldThrowObjectDisposedGivenDisposedTableWhenSubscribing()
    {
        // Arrange
        var fake = new FakeWire();
        var table = fake.CreateTable();
        await table.DisposeAsync();

        // Act
        var act = () => table.SubscribeAsync("kv://a/b/*", Ignore, preDispatch: null, CancellationToken.None);

        // Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(act);
    }

    static ValueTask Ignore(string value, CancellationToken ct) => ValueTask.CompletedTask;

    sealed class FakeWire
    {
        Action<ReadOnlyMemory<byte>>? _notify;

        internal int SubscribeCalls { get; private set; }

        internal Task<ulong>? RestoreReply { get; init; }

        internal int FailSubscribeCall { get; init; }

        internal List<(string Pattern, ulong SubscriptionId)> Unsubscribed { get; } = [];

        internal SubscriptionTable<string> CreateTable() => new(
            this,
            "kv",
            notifyMessageType: 1,
            (_, handler) =>
            {
                _notify = handler;
                return new TestRegistration();
            },
            dispatchAsyncHandler: null,
            bufferCapacity: 16,
            (_, _, _) =>
            {
                SubscribeCalls++;

                if (SubscribeCalls > 1 && RestoreReply is not null)
                    return RestoreReply;

                return SubscribeCalls == FailSubscribeCall
                    ? Task.FromException<ulong>(new InvalidOperationException("restore rejected"))
                    : Task.FromResult((ulong)SubscribeCalls);
            },
            (pattern, subscriptionId, _, _) =>
            {
                Unsubscribed.Add((pattern, subscriptionId));
                return Task.CompletedTask;
            },
            static payload =>
            {
                var subscriptionId = BitConverter.ToUInt64(payload.Span[..8]);
                var value = System.Text.Encoding.UTF8.GetString(payload.Span[8..]);
                return new DecodedNotification<string>(subscriptionId, "kv://a/b/c", value);
            });

        internal void Deliver(ulong subscriptionId, string value)
        {
            byte[] payload = [.. BitConverter.GetBytes(subscriptionId), .. System.Text.Encoding.UTF8.GetBytes(value)];
            _notify!(payload);
        }
    }
}
