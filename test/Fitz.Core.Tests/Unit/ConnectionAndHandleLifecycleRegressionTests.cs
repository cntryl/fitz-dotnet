using System.Threading.Tasks.Sources;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ConnectionAndHandleLifecycleRegressionTests
{
    [Fact]
    public async Task ShouldPreserveNewSessionGivenOldHandleWhenSendingBoundRequest()
    {
        // Arrange
        await using var first = new TestQueuedTransport();
        await using var second = new TestQueuedTransport();
        var factoryCalls = 0;
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://queued/ws"),
            Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(1), MaxBackoff: TimeSpan.FromMilliseconds(1)),
            Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt =>
            {
                if (evt.Event == "reconnect_succeeded")
                    reconnected.TrySetResult();
            })), () => Interlocked.Increment(ref factoryCalls) == 1 ? first : second);
        await connection.ConnectAsync();
        first.QueueResponse(MessageTypes.KvBegin, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 9 });
        var response = await connection.RequestInSessionAsync(MessageTypes.KvBegin, ReadOnlyMemory<byte>.Empty);
        first.QueueClosed();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(3));
        var sentBefore = second.SentFrames.Count;

        // Act
        await Assert.ThrowsAsync<ConnectionException>(() => connection.RequestOnSessionAsync(
            MessageTypes.KvGet, ReadOnlyMemory<byte>.Empty, response.Closed, CancellationToken.None).AsTask());

        // Assert
        Assert.True(response.Closed.IsCancellationRequested);
        Assert.Equal(sentBefore, second.SentFrames.Count);
        Assert.Equal(ConnectionState.Authenticated, connection.State);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShouldResetConnectionGivenInterruptedFrameWriteWhenSending(bool request)
    {
        // Arrange
        using var cancellation = new CancellationTokenSource();
        await using var transport = new InterruptedWriteTransport(cancellation);
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://queued/ws"),
            Reconnect: new ReconnectOptions(false)), () => transport);
        await connection.ConnectAsync();
        transport.FailWrites = true;

        // Act
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request
            ? connection.RequestAsync(MessageTypes.KvGet, ReadOnlyMemory<byte>.Empty, cancellation.Token).AsTask()
            : connection.SendAsync(MessageTypes.NoticePublish, ReadOnlyMemory<byte>.Empty, cancellation.Token).AsTask());

        // Assert
        await transport.Closed.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(ConnectionState.Disconnected, connection.State);
    }

    [Fact]
    public async Task ShouldCompleteCloseGivenThrowingWorkerCancellationWhenClosingConnection()
    {
        // Arrange
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://unused/ws"), Reconnect: new ReconnectOptions(false)), () => throw new InvalidOperationException());
        Action<byte[]>? receive = null;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var rpc = new RpcClient((_, _, _) => Task.FromResult(new byte[] { 0, 0, 0, 0, 0 }),
            send: (_, _, _) => Task.CompletedTask,
            registerNotificationHandler: (kind, handler) =>
            {
                if (kind == MessageTypes.RpcRequest)
                    receive = handler;
                return new TestRegistration();
            }, getConnectionClosedToken: () => connection.ConnectionClosedToken);
        await using var worker = await rpc.RegisterWorkerAsync("rpc://prod/app/work", async (_, _, ct) =>
        {
            using var callback = ct.Register(() => throw new InvalidOperationException("Application cancellation callback failed."));
            started.TrySetResult();
            await release.Task;
        });
        using var writer = new BinaryBufferWriter();
        writer.WriteBytes(new byte[16]);
        writer.WriteString("rpc://prod/app/work");
        writer.WriteU32(0);
        writer.WriteU8(0);
        receive!(writer.Build());
        await started.Task.WaitAsync(TimeSpan.FromSeconds(3));

        // Act
        Exception? failure;
        try
        {
            failure = await Record.ExceptionAsync(() => connection.CloseAsync());
        }
        finally
        {
            release.TrySetResult();
        }

        // Assert
        Assert.Null(failure);
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public async Task ShouldReconnectGivenThrowingCancellationWhenTransportIsLost()
    {
        // Arrange
        await using var first = new TestQueuedTransport();
        await using var second = new TestQueuedTransport();
        var factoryCalls = 0;
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = new FitzConnection(new ClientConfig(new Uri("ws://queued/ws"),
            Timeout: TimeSpan.FromSeconds(1),
            Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(1), MaxBackoff: TimeSpan.FromMilliseconds(1)),
            Observability: new FitzObservabilityOptions(OnLifecycleEvent: evt =>
            {
                if (evt.Event == "reconnect_succeeded")
                    reconnected.TrySetResult();
            })), () => Interlocked.Increment(ref factoryCalls) == 1 ? first : second);
        await connection.ConnectAsync();
        using var callback = connection.ConnectionClosedToken.Register(() => throw new InvalidOperationException("Application cancellation callback failed."));

        // Act
        first.QueueClosed();

        // Assert
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(2, factoryCalls);
        Assert.Equal(ConnectionState.Authenticated, connection.State);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShouldCloseLeaseGivenUncertainRenewalWhenRenewingAgain(bool malformed)
    {
        // Arrange
        var renewals = 0;
        await using var client = new LeaseClient((type, _, _) =>
        {
            if (type == MessageTypes.LeaseAcquire)
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU8(1);
                writer.WriteU64(77);
                return Task.FromResult(writer.Build());
            }
            if (type == MessageTypes.LeaseRenew)
            {
                renewals++;
                return malformed ? Task.FromResult(new byte[] { 0 }) : Task.FromException<byte[]>(new RequestTimeoutException("Sent renewal reply was lost."));
            }
            return Task.FromResult(new byte[] { 0 });
        });
        await using var lease = await client.AcquireAsync("lease://prod/app/lock", TimeSpan.FromSeconds(30));

        // Act
        var firstFailure = await Record.ExceptionAsync(() => lease.ExtendAsync(TimeSpan.FromSeconds(30)));

        // Assert
        Assert.NotNull(firstFailure);
        var closed = await Assert.ThrowsAsync<LeaseException>(() => lease.ExtendAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal("CLOSED", closed.Code);
        Assert.Equal(1, renewals);
    }

    [Fact]
    public async Task ShouldRejectSessionGivenDisconnectAfterSuccessfulBeginReplyWhenConstructingHandle()
    {
        // Arrange
        using var response = new PausedResponse();
        var listeners = new List<Action>();
        await using var client = new StreamClient((_, _, _) => response.Task,
            registerOnDisconnect: action =>
            {
                listeners.Add(action);
                return new TestRegistration(() => listeners.Remove(action));
            });
        var begin = client.BeginAsync("stream://prod/app/events");
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        writer.WriteU64(9);
        writer.WriteU32(0);
        response.Complete(writer.Build());
        await response.Resuming.Task.WaitAsync(TimeSpan.FromSeconds(3));

        // Act
        foreach (var listener in listeners.ToArray())
            listener();
        response.AllowResume.Set();

        // Assert
        await Assert.ThrowsAsync<ConnectionException>(() => begin);
    }

    sealed class PausedResponse : IValueTaskSource<ReadOnlyMemory<byte>>, IDisposable
    {
        ManualResetValueTaskSourceCore<ReadOnlyMemory<byte>> _source = new() { RunContinuationsAsynchronously = true };
        internal TaskCompletionSource Resuming { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal ManualResetEventSlim AllowResume { get; } = new();
        internal ValueTask<ReadOnlyMemory<byte>> Task => new(this, _source.Version);
        internal void Complete(byte[] reply) => _source.SetResult(reply);
        public ReadOnlyMemory<byte> GetResult(short token)
        {
            var result = _source.GetResult(token);
            Resuming.TrySetResult();
            if (!AllowResume.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Continuation was not released.");
            return result;
        }
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
        public void Dispose() => AllowResume.Dispose();
    }

    sealed class InterruptedWriteTransport(CancellationTokenSource cancellation) : ITransport
    {
        readonly TestQueuedTransport _inner = new();
        internal bool FailWrites { get; set; }
        internal TaskCompletionSource Closed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Uri Url => _inner.Url;
        public Task ConnectAsync(CancellationToken ct = default) => _inner.ConnectAsync(ct);
        public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            if (FailWrites)
            {
                await cancellation.CancelAsync();
                throw new TransportSendCanceledException(new OperationCanceledException(ct), ct);
            }
            await _inner.SendAsync(data, ct);
        }
        public ValueTask<PooledFrame> ReceiveAsync(CancellationToken ct = default) => _inner.ReceiveAsync(ct);
        public async Task CloseAsync(CancellationToken ct = default)
        {
            await _inner.CloseAsync(ct);
            Closed.TrySetResult();
        }
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
