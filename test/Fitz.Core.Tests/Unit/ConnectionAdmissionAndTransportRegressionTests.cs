using System.Net;
using System.Net.Sockets;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ConnectionAdmissionAndTransportRegressionTests
{
    [Fact]
    public async Task ShouldFreeQueueCapacityGivenCanceledWaiterWhenReplacingRequest()
    {
        // Arrange
        var gate = new RequestGate(maxConcurrency: 1, maxQueueSize: 1);
        using var active = await gate.AcquireAsync(CancellationToken.None);
        using var canceled = new CancellationTokenSource();
        var waiting = gate.AcquireAsync(canceled.Token).AsTask();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        using var replacementCancellation = new CancellationTokenSource();

        // Act
        var replacement = gate.AcquireAsync(replacementCancellation.Token).AsTask();

        // Assert
        Assert.False(replacement.IsCompleted);
        await replacementCancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => replacement);
    }

    [Fact]
    public async Task ShouldResetSessionGivenUnansweredCancellationBudgetWhenCorrelationsAccumulate()
    {
        // Arrange
        var reset = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var mux = new Multiplexer(onSessionReset: error => reset.TrySetResult(error), maxCorrelationTombstones: 2);
        mux.SetConnected();
        var unrelated = mux.RequestAsync(
            MessageTypes.KvGet, [], (_, _) => Task.CompletedTask,
            Timeout.InfiniteTimeSpan, correlationId: 100_000);

        // Act
        for (ulong correlation = 1; correlation <= 3; correlation++)
        {
            using var cancellation = new CancellationTokenSource();
            var request = mux.RequestAsync(
                MessageTypes.KvGet, [], (_, _) => Task.CompletedTask,
                Timeout.InfiniteTimeSpan, correlationId: correlation, ct: cancellation.Token);
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request);
        }

        // Assert
        Assert.True(reset.Task.IsCompleted, "Exhausting the retained-correlation budget must reset the session.");
        Assert.IsType<ConnectionException>(await reset.Task);
        await Assert.ThrowsAsync<ConnectionException>(() => unrelated);
        await Assert.ThrowsAsync<ConnectionException>(() => mux.RequestAsync(
            MessageTypes.KvGet, [], (_, _) => Task.CompletedTask,
            Timeout.InfiniteTimeSpan, correlationId: 200_000));
    }

    [Fact]
    public async Task ShouldRetainLateResponseIdentityGivenCanceledRequestWhenIntermediateResponseArrives()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cancellation = new CancellationTokenSource();
        var request = mux.RequestAsync(
            MessageTypes.LeaseAcquire, [], (_, _) => Task.CompletedTask,
            Timeout.InfiniteTimeSpan, correlationId: 77, ct: cancellation.Token);
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request);

        // Act
        var intermediate = Record.Exception(() => mux.DispatchCorrelated(77, new byte[] { 0, 2 }, responseFinal: false));
        var terminal = Record.Exception(() => mux.DispatchCorrelated(77, new byte[] { 0 }, responseFinal: true));

        // Assert
        Assert.Null(intermediate);
        Assert.Null(terminal);
        Assert.Throws<ProtocolException>(() => mux.DispatchCorrelated(77, new byte[] { 0 }));
    }

    [Fact]
    public async Task ShouldInvalidateTransportGivenCanceledFrameWriteWhenSendingAgain()
    {
        // Arrange
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        await using var transport = new TcpTransport(
            new Uri($"tcp://127.0.0.1:{port}"), TimeSpan.FromSeconds(5), FitzLimits.MaxFrameSize);
        var accepted = listener.AcceptTcpClientAsync();
        await transport.ConnectAsync();
        using var server = await accepted;
        var frame = FrameCodec.Encode(MessageTypes.NoticePublish, new byte[FitzLimits.MaxFrameSize - FrameCodec.MaxHeaderSize]);
        var canceled = false;

        // Act
        // The peer deliberately does not read until cancellation. Every frame stays within
        // the protocol limit; a bounded series of writes fills the socket send buffers.
        for (var i = 0; i < 64; i++)
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            try
            {
                await transport.SendAsync(frame, cancellation.Token);
            }
            catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
            {
                canceled = true;
                break;
            }
        }
        Assert.True(canceled, "The bounded write series must reach socket backpressure.");
        var drain = Task.Run(async () =>
        {
            var buffer = new byte[64 * 1024];
            try
            {
                while (await server.GetStream().ReadAsync(buffer) != 0)
                {
                }
            }
            catch (IOException)
            {
                // An aborted partial write may close with either FIN or RST.
            }
        });

        // Assert
        try
        {
            await Assert.ThrowsAsync<ConnectionException>(() => transport.SendAsync(
                FrameCodec.Encode(MessageTypes.NoticePublish, new byte[] { 0xAA, 0xBB })));
        }
        finally
        {
            await transport.CloseAsync();
            await drain.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }
}
