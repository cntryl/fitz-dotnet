using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ClientLifecycleReviewTests
{
    static readonly TimeSpan Deadline = TimeSpan.FromSeconds(3);

    [Fact]
    public async Task ShouldRestoreConnectionGivenPendingSubscriptionWhenTransportReconnects()
    {
        // Arrange
        await using var original = new TestQueuedTransport();
        await using var replacement = new ControlledConnectTransport();
        var attempts = 0;
        var config = new ClientConfig(original.Url)
        {
            Timeout = Timeout.InfiniteTimeSpan,
            Reconnect = new ReconnectOptions(Backoff: TimeSpan.FromMilliseconds(1)),
        };
        await using var connection = new FitzConnection(config,
            () => Interlocked.Increment(ref attempts) == 1 ? original : replacement);
        await using var notice = new NoticeClient(connection);
        using var cancelSubscription = new CancellationTokenSource();
        using var reply = new BinaryBufferWriter();
        reply.WriteU8(0);
        reply.WriteU8(1);
        reply.WriteU64(42);
        replacement.QueueResponse(MessageTypes.NoticeSubscribe, reply.WrittenMemory.Span);
        replacement.QueueResponse(MessageTypes.NoticeUnsubscribe, new byte[] { 0 });
        await connection.ConnectAsync().WaitAsync(Deadline);

        // Act
        original.QueueClosed();
        await replacement.Connecting.Task.WaitAsync(Deadline);
        var subscribing = notice.SubscribeAsync("notice://prod/app/events", cancelSubscription.Token);
        replacement.Resume.TrySetResult();

        // Assert
        try
        {
            await using var subscription = await subscribing.WaitAsync(Deadline);
            Assert.Equal(ConnectionState.Authenticated, connection.State);
        }
        finally
        {
            await cancelSubscription.CancelAsync();
            await Record.ExceptionAsync(() => subscribing.WaitAsync(Deadline));
        }
    }

    [Fact]
    public async Task ShouldReleaseSiblingGivenDisconnectedTransportWhenUnsubscribing()
    {
        // Arrange
        await using var transport = new TestQueuedTransport();
        var lost = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var config = new ClientConfig(transport.Url)
        {
            Reconnect = new ReconnectOptions(Enabled: false),
            Observability = new FitzObservabilityOptions(OnLifecycleEvent: evt =>
            {
                if (evt.Event == "connection_lost")
                    lost.TrySetResult();
            }),
        };
        await using var connection = new FitzConnection(config, () => transport);
        await using var notice = new NoticeClient(connection);
        using var reply = new BinaryBufferWriter();
        reply.WriteU8(0);
        reply.WriteU8(1);
        reply.WriteU64(42);
        transport.QueueResponse(MessageTypes.NoticeSubscribe, reply.WrittenMemory.Span);
        await connection.ConnectAsync().WaitAsync(Deadline);
        const string pattern = "notice://prod/app/events";
        await using var first = await notice.SubscribeAsync(pattern).WaitAsync(Deadline);
        await using var sibling = await notice.SubscribeAsync(pattern).WaitAsync(Deadline);

        // Act
        transport.QueueClosed();
        await lost.Task.WaitAsync(Deadline);
        await first.UnsubscribeAsync().AsTask().WaitAsync(Deadline);

        // Assert
        await first.Completion.WaitAsync(Deadline);
        Assert.False(sibling.Completion.IsCompleted);
    }

    [Fact]
    public async Task ShouldShareShutdownGivenConcurrentCloseWhenCallerCancelsWait()
    {
        // Arrange
        await using var transport = new ControlledCloseTransport();
        await using var client = new FitzClient(new ClientConfig(transport.Url) { TransportFactory = _ => transport });
        using var firstCaller = new CancellationTokenSource();
        await client.ConnectAsync().WaitAsync(Deadline);
        var first = client.CloseAsync(firstCaller.Token);
        await transport.Closing.Task.WaitAsync(Deadline);

        // Act
        var second = client.DisposeAsync().AsTask();
        await firstCaller.CancelAsync();

        // Assert
        try
        {
            Assert.False(second.IsCompleted);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(Deadline));
        }
        finally
        {
            transport.Resume.TrySetResult();
            await second.WaitAsync(Deadline);
            await Record.ExceptionAsync(() => first.WaitAsync(Deadline));
        }
        Assert.Equal(1, transport.CloseCount);
        Assert.False(typeof(IDisposable).IsAssignableFrom(typeof(FitzClient)));
    }

    [Fact]
    public void ShouldAvoidReflectionGivenCompiledCoreWhenInspectingVersionLookup()
    {
        // Arrange
        using var assembly = File.OpenRead(typeof(FitzClient).Assembly.Location);
        using var reader = new PEReader(assembly);
        var metadata = reader.GetMetadataReader();

        // Act
        var references = metadata.TypeReferences.Select(metadata.GetTypeReference);

        // Assert
        Assert.DoesNotContain(references, type =>
            metadata.StringComparer.Equals(type.Namespace, "System.Reflection") &&
            metadata.StringComparer.Equals(type.Name, "CustomAttributeExtensions"));
    }

    sealed class ControlledConnectTransport : TestQueuedTransport
    {
        internal TaskCompletionSource Connecting { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async Task ConnectAsync(CancellationToken ct = default)
        {
            Connecting.TrySetResult();
            await Resume.Task.WaitAsync(ct);
            await base.ConnectAsync(ct);
        }
    }

    sealed class ControlledCloseTransport : ITransport
    {
        readonly TestQueuedTransport _transport = new();
        internal TaskCompletionSource Closing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int CloseCount { get; private set; }
        public Uri Url => _transport.Url;
        public Task ConnectAsync(CancellationToken ct = default) => _transport.ConnectAsync(ct);
        public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default) => _transport.SendAsync(data, ct);
        public ValueTask<PooledFrame> ReceiveAsync(CancellationToken ct = default) => _transport.ReceiveAsync(ct);
        public ValueTask DisposeAsync() => _transport.DisposeAsync();

        public async Task CloseAsync(CancellationToken ct = default)
        {
            CloseCount++;
            Closing.TrySetResult();
            await Resume.Task.WaitAsync(ct);
            await _transport.CloseAsync(ct);
        }
    }
}
