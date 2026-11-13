using Cntryl.Fitz.Connection;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerCleanupTests
{
    [Fact]
    public async Task ShouldCleanUpOnSendFailureGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        // Act
        var act = () => mux.RequestAsync(
            100,
            [1, 2, 3],
            static async (_, _) =>
            {
                await Task.CompletedTask;
                throw new InvalidOperationException("Send failed");
            },
            TimeSpan.FromSeconds(5),
            correlationId: 1);

        // Assert
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(act);
        Assert.Equal("Send failed", ex.Message);
    }

    [Fact]
    public async Task ShouldAllowNextRequestAfterSendFailureGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var firstTask = mux.RequestAsync(
            101,
            [0x1],
            static async (_, _) =>
            {
                await Task.CompletedTask;
                throw new InvalidOperationException("First failed");
            },
            TimeSpan.FromSeconds(1),
            correlationId: 1);
        await Assert.ThrowsAsync<InvalidOperationException>(() => firstTask);

        // Act
        var secondTask = mux.RequestAsync(
            101,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 1);
        mux.DispatchCorrelated(1, new byte[] { 0xA });

        // Assert
        Assert.Equal([0xA], await secondTask);
    }

    [Fact]
    public async Task ShouldNotDispatchToTimedOutRequestGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var requestTask = mux.RequestAsync(
            102,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50),
            correlationId: 1);
        await Assert.ThrowsAsync<RequestTimeoutException>(() => requestTask);

        // Act
        var lateReply = Record.Exception(() => mux.DispatchCorrelated(1, new byte[] { 0xB }));

        // Assert
        Assert.Null(lateReply);
    }

    [Fact]
    public async Task ShouldHandleCancellationBeforeSendGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();

        // Act
        var requestTask = mux.RequestAsync(
            103,
            [0x1],
            async (_, _) => await cts.CancelAsync(),
            TimeSpan.FromSeconds(5),
            correlationId: 1,
            ct: cts.Token);

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task ShouldHandleCancellationAfterSendGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();
        var requestTask = mux.RequestAsync(
            104,
            [0x1],
            static async (_, token) => await Task.Delay(100, token),
            TimeSpan.FromSeconds(5),
            correlationId: 1,
            ct: cts.Token);
        await Task.Delay(50);

        // Act
        await cts.CancelAsync();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task ShouldKeepSessionGivenCorrelatedRequestWhenCanceledAfterSend()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cancellation = new CancellationTokenSource();
        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceledRequest = mux.RequestAsync(
            109,
            [0x1],
            (_, _) =>
            {
                sent.TrySetResult();
                return Task.CompletedTask;
            },
            Timeout.InfiniteTimeSpan,
            correlationId: 1,
            ct: cancellation.Token);
        await sent.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Act
        await cancellation.CancelAsync();
        var nextRequest = mux.RequestAsync(
            109,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 2);
        mux.DispatchCorrelated(1, new byte[] { 0xA });
        mux.DispatchCorrelated(2, new byte[] { 0xB });

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledRequest);
        Assert.Equal([0xB], await nextRequest);
    }

    [Fact]
    public async Task ShouldCancelAllOnDisconnectGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var first = mux.RequestAsync(
            106,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            correlationId: 1);
        var second = mux.RequestAsync(
            106,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            correlationId: 2);

        // Act
        mux.SetDisconnected();

        // Assert
        await Assert.ThrowsAsync<ConnectionException>(() => first);
        await Assert.ThrowsAsync<ConnectionException>(() => second);
    }

    [Fact]
    public async Task ShouldKeepLongRequestGivenShortRequestWhenShortTimesOut()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var shortTimeout = mux.RequestAsync(
            107,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50),
            correlationId: 1);
        var longTimeout = mux.RequestAsync(
            108,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            correlationId: 2);

        // Act
        await Assert.ThrowsAsync<RequestTimeoutException>(() => shortTimeout);
        mux.DispatchCorrelated(2, new byte[] { 0xC });

        // Assert
        Assert.Equal([0xC], await longTimeout);
    }

    [Fact]
    public async Task ShouldNotLeakOnRapidTimeoutsGivenActiveMultiplexerWhenRequestCompletes()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        for (ulong i = 1; i <= 10; i++)
        {
            var task = mux.RequestAsync(
                (ushort)(200 + i),
                [0x1],
                static (_, _) => Task.CompletedTask,
                TimeSpan.FromMilliseconds(10),
                correlationId: i);
            await Assert.ThrowsAsync<RequestTimeoutException>(() => task);
        }

        // Act
        var finalTask = mux.RequestAsync(
            210,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            correlationId: 11);
        mux.DispatchCorrelated(11, new byte[] { 0xD });

        // Assert
        Assert.Equal([0xD], await finalTask);
    }
}
