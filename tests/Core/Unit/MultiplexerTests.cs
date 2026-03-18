using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerTests
{
    [Fact]
    public async Task RequestAsync_ResolvesWhenDispatched()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        var task = mux.RequestAsync(
            100,
            [1, 2, 3],
            static (data, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(100, [9, 8, 7]);

        var result = await task;
        Assert.Equal([9, 8, 7], result);
    }

    [Fact]
    public async Task RequestAsync_TimeoutsWhenNoDispatch()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            mux.RequestAsync(
                101,
                [0x1],
                static (data, _) => Task.CompletedTask,
                TimeSpan.FromMilliseconds(50)
            )
        );
    }
}