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
}