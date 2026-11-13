using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SubscriptionTableTests
{
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
            (_, _) =>
            {
                SubscribeCalls++;
                return SubscribeCalls == FailSubscribeCall
                    ? Task.FromException<ulong>(new InvalidOperationException("restore rejected"))
                    : Task.FromResult((ulong)SubscribeCalls);
            },
            (pattern, subscriptionId, _) =>
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
