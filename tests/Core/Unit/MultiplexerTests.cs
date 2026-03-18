using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerTests
{
    [Fact]
    public async Task should_resolve_response_given_dispatched_message_when_requesting()
    {
        // Arrange
        var mux = new Multiplexer();
        mux.SetConnected();

        var task = mux.RequestAsync(
            100,
            [1, 2, 3],
            static (data, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        // Act
        mux.Dispatch(100, [9, 8, 7]);

        // Assert
        var result = await task;
        Assert.Equal([9, 8, 7], result);
    }

    [Fact]
    public async Task should_throw_timeout_given_missing_dispatch_when_requesting()
    {
        // Arrange
        var mux = new Multiplexer();
        mux.SetConnected();

        // Act
        var act = () => mux.RequestAsync(
            101,
            [0x1],
            static (data, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50)
        );

        // Assert
        await Assert.ThrowsAsync<RequestTimeoutException>(act);
    }

    [Fact]
    public async Task should_throw_operation_canceled_given_canceled_token_when_requesting()
    {
        // Arrange
        var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act
        var act = () => mux.RequestAsync(
            102,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            cts.Token
        );

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
    }

    [Fact]
    public async Task should_match_fifo_order_given_two_inflight_requests_when_dispatching_same_message_type()
    {
        // Arrange
        var mux = new Multiplexer();
        mux.SetConnected();

        var firstTask = mux.RequestAsync(
            120,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        var secondTask = mux.RequestAsync(
            120,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        // Act
        mux.Dispatch(120, [0xA]);
        mux.Dispatch(120, [0xB]);

        // Assert
        Assert.Equal([0xA], await firstTask);
        Assert.Equal([0xB], await secondTask);
    }

    [Fact]
    public async Task should_cancel_inflight_request_given_disconnect_when_cancel_all_is_called()
    {
        // Arrange
        var mux = new Multiplexer();
        mux.SetConnected();

        var inflight = mux.RequestAsync(
            121,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(10)
        );

        // Act
        mux.SetDisconnected();

        // Assert
        await Assert.ThrowsAsync<ConnectionException>(() => inflight);
    }
}