using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class PublicSurfaceTests
{
    [Fact]
    public async Task ShouldThrowNonRetryableDisposedErrorGivenClosedClientWhenConnecting()
    {
        // Arrange
        await using var client = new FitzClient(new ClientConfig(new Uri("ws://closed/ws")));
        await client.CloseAsync();

        // Act
        var error = await Record.ExceptionAsync(() => client.ConnectAsync());

        // Assert
        Assert.IsType<ObjectDisposedException>(error);
        Assert.False(Retryability.IsRetryable(error));
    }

    [Theory]
    [InlineData(typeof(SubscriptionBackpressureException))]
    [InlineData(typeof(AsyncHandlerOverflowException))]
    public void ShouldDeriveFromFitzExceptionGivenSubscriptionDeliveryFailureWhenCatching(Type exceptionType)
    {
        // Arrange
        var expected = typeof(FitzException);

        // Act
        var isFitzException = expected.IsAssignableFrom(exceptionType);

        // Assert
        Assert.True(isFitzException);
    }

    [Fact]
    public async Task ShouldRejectOperationsGivenDisposedPublicDomainClientsWhenBeforeTransport()
    {
        // Arrange
        var transportCalls = 0;
        Task<byte[]> Request(ushort messageType, byte[] payload, CancellationToken ct)
        {
            transportCalls++;
            return Task.FromResult(Array.Empty<byte>());
        }

        var kv = new KvClient(Request);
        var lease = new LeaseClient(Request);
        var notice = new NoticeClient((_, _, _) => { transportCalls++; return Task.CompletedTask; });
        var queue = new QueueClient(Request);
        var rpc = new RpcClient(Request);
        var schedule = new ScheduleClient(Request);
        var stream = new StreamClient(Request);
        await kv.DisposeAsync();
        await lease.DisposeAsync();
        await notice.DisposeAsync();
        await queue.DisposeAsync();
        rpc.Dispose();
        await schedule.DisposeAsync();

        // Act
        await stream.DisposeAsync();


        // Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(() => kv.BeginAsync("kv://prod/app/data"));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => lease.QueryAsync("lease://prod/app/resource"));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => notice.PublishAsync("notice://prod/app/event", ReadOnlyMemory<byte>.Empty));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => queue.EnqueueAsync("queue://prod/app/tasks", ReadOnlyMemory<byte>.Empty));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => rpc.RegisterWorkerAsync("rpc://prod/app/*", (_, _, _) => ValueTask.CompletedTask));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => schedule.CancelAsync("schedule://prod/app/job"));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => stream.PeekAsync("stream://prod/app/events"));

        Assert.Equal(0, transportCalls);
    }

    [Fact]
    public void ShouldRejectNullTransportDelegateGivenPublicDomainClientConstructionWhenPublicApiRuns()
    {
        // Arrange
        // Act
        // Assert
        Assert.Throws<ArgumentNullException>(() => new KvClient((Func<ushort, byte[], CancellationToken, Task<byte[]>>)null!));
        Assert.Throws<ArgumentNullException>(() => new LeaseClient((Func<ushort, byte[], CancellationToken, Task<byte[]>>)null!));
        Assert.Throws<ArgumentNullException>(() => new NoticeClient((Func<ushort, byte[], CancellationToken, Task>)null!));
        Assert.Throws<ArgumentNullException>(() => new QueueClient((Func<ushort, byte[], CancellationToken, Task<byte[]>>)null!));
        Assert.Throws<ArgumentNullException>(() => new RpcClient((Func<ushort, byte[], CancellationToken, Task<byte[]>>)null!));
        Assert.Throws<ArgumentNullException>(() => new ScheduleClient((Func<ushort, byte[], CancellationToken, Task<byte[]>>)null!));
        Assert.Throws<ArgumentNullException>(() => new StreamClient((Func<ushort, byte[], CancellationToken, Task<byte[]>>)null!));
    }

    [Fact]
    public async Task ShouldAllowRetryGivenUnsubscribeFailureWhenPublicApiRuns()
    {
        // Arrange
        var expected = new InvalidOperationException("unsubscribe failed");
        var attempts = 0;

        // Act
        await using var handle = new TestSubscriptionHandle(_ =>
        {
            attempts++;
            return attempts == 1 ? ValueTask.FromException(expected) : ValueTask.CompletedTask;
        });


        // Assert
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => handle.UnsubscribeAsync().AsTask());
        await handle.UnsubscribeAsync();
        await handle.Completion;

        Assert.Same(expected, thrown);
        Assert.Equal(2, attempts);
    }

    sealed class TestSubscriptionHandle(Func<CancellationToken, ValueTask> unsubscribe)
        : SubscriptionHandle("notice://realm/area/resource", unsubscribe)
    {
    }
}
