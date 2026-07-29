using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerCleanupTests
{
    [Fact]
    public async Task should_clean_up_on_send_failure()
    {
        using var mux = new Multiplexer();
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
        using var mux = new Multiplexer();
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
        using var mux = new Multiplexer();
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
    public async Task should_not_deliver_stale_response_to_following_request_after_timeout()
    {
        using var mux = new Multiplexer();
        mux.SetConnected();

        var timedOutRequest = mux.RequestAsync(
            108,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(20)
        );

        await Assert.ThrowsAnyAsync<RequestTimeoutException>(() => timedOutRequest);

        var followUp = mux.RequestAsync(
            108,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(108, [0xAA]);
        mux.Dispatch(108, [0xBB]);

        Assert.Equal([0xBB], await followUp);
    }

    [Fact]
    public async Task should_handle_cancellation_before_send()
    {
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();

        var requestTask = mux.RequestAsync(
            103,
            [0x1],
            async (_, _) =>
            {
                await cts.CancelAsync();
            },
            TimeSpan.FromSeconds(5),
            cancellationToken: cts.Token
        );

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task should_handle_cancellation_after_send()
    {
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();

        var requestTask = mux.RequestAsync(
            104,
            [0x1],
            static async (_, token) => await Task.Delay(100, token),
            TimeSpan.FromSeconds(5),
            cancellationToken: cts.Token
        );

        await Task.Delay(50);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requestTask);
    }

    [Fact]
    public async Task should_serialize_concurrent_requests_and_handle_cancel()
    {
        using var mux = new Multiplexer();
        mux.SetConnected();
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var firstCts = new CancellationTokenSource();
        var first = mux.RequestAsync(
            105,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            cancellationToken: firstCts.Token
        );

        await Task.Delay(10);

        var second = mux.RequestAsync(
            105,
            [0x2],
            (_, _) =>
            {
                secondStarted.TrySetResult();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(5)
        );

        await firstCts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await secondStarted.Task;

        mux.Dispatch(105, [0xB]);
        var result = await second;
        Assert.Equal([0xB], result);
    }

    [Fact]
    public async Task should_cancel_all_on_disconnect()
    {
        using var mux = new Multiplexer();
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
        using var mux = new Multiplexer();
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
        using var mux = new Multiplexer();
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
