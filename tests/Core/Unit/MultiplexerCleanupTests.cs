using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerCleanupTests
{
    [Fact]
    public async Task should_clean_up_on_send_failure()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        var act = () => mux.RequestAsync(
            100,
            [1, 2, 3],
            async (data, _) =>
            {
                await Task.CompletedTask;
                throw new InvalidOperationException("Send failed");
            },
            TimeSpan.FromSeconds(5)
        );

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(act);
        Assert.Equal("Send failed", ex.Message);
    }

    [Fact]
    public async Task should_allow_next_request_after_send_failure()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        var firstTask = mux.RequestAsync(
            101,
            [0x1],
            async (data, _) =>
            {
                await Task.CompletedTask;
                throw new InvalidOperationException("First failed");
            },
            TimeSpan.FromSeconds(1)
        );

        await Assert.ThrowsAsync<InvalidOperationException>(() => firstTask);

        var secondTask = mux.RequestAsync(
            101,
            [0x2],
            async (data, _) => await Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(101, [0xA]);
        var result = await secondTask;
        Assert.Equal([0xA], result);
    }

    [Fact]
    public async Task should_not_dispatch_to_timed_out_request()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        var requestTask = mux.RequestAsync(
            102,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50)
        );

        await Assert.ThrowsAsync<RequestTimeoutException>(() => requestTask);

        // Dispatch after timeout should not throw
        mux.Dispatch(102, [0xB]);
    }

    [Fact]
    public async Task should_handle_cancellation_before_send()
    {
        var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();

        var requestTask = mux.RequestAsync(
            103,
            [0x1],
            (_, _) =>
            {
                cts.Cancel();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(5),
            cts.Token
        );

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task should_handle_cancellation_after_send()
    {
        var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();

        var requestTask = mux.RequestAsync(
            104,
            [0x1],
            static async (_, _) => await Task.Delay(100),
            TimeSpan.FromSeconds(5),
            cts.Token
        );

        await Task.Delay(50);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task should_serialize_concurrent_requests_and_handle_cancel()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        using var firstCts = new CancellationTokenSource();
        var first = mux.RequestAsync(
            105,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            firstCts.Token
        );

        await Task.Delay(10);

        var second = mux.RequestAsync(
            105,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5)
        );

        firstCts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);

        mux.Dispatch(105, [0xB]);
        var result = await second;
        Assert.Equal([0xB], result);
    }

    [Fact]
    public async Task should_cancel_all_on_disconnect()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        // Use short timeout combined with disconnect to ensure shutdown message wins
        var first = mux.RequestAsync(
            106,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(500)
        );

        var second = mux.RequestAsync(
            106,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(500)
        );

        mux.SetDisconnected();

        // Both should fail - with either ConnectionException or timeout
        // depending on timing/ordering
        var firstFailed = false;
        var secondFailed = false;

        try
        {
            await first;
        }
        catch (ConnectionException)
        {
            firstFailed = true;
        }
        catch (RequestTimeoutException)
        {
            firstFailed = true;
        }

        try
        {
            await second;
        }
        catch (ConnectionException)
        {
            secondFailed = true;
        }
        catch (RequestTimeoutException)
        {
            secondFailed = true;
        }

        Assert.True(firstFailed && secondFailed, "Both requests should have failed");
    }

    [Fact]
    public async Task should_handle_mixed_timeout_durations()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        var shortTimeout = mux.RequestAsync(
            107,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(50)
        );

        await Task.Delay(10);

        var longTimeout = mux.RequestAsync(
            108,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5)
        );

        await Assert.ThrowsAsync<RequestTimeoutException>(() => shortTimeout);

        mux.Dispatch(108, [0xC]);
        var result = await longTimeout;
        Assert.Equal([0xC], result);
    }

    [Fact]
    public async Task should_not_leak_on_rapid_timeouts()
    {
        var mux = new Multiplexer();
        mux.SetConnected();

        for (int i = 0; i < 10; i++)
        {
            var task = mux.RequestAsync(
                (ushort)(200 + i),
                [0x1],
                static (_, _) => Task.CompletedTask,
                TimeSpan.FromMilliseconds(10)
            );

            await Assert.ThrowsAsync<RequestTimeoutException>(() => task);
        }

        var finalTask = mux.RequestAsync(
            210,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(210, [0xD]);
        var result = await finalTask;
        Assert.Equal([0xD], result);
    }
}
