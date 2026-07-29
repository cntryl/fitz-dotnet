using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class MultiplexerTests
{
    [Fact]
    public async Task should_resolve_response_given_dispatched_message_when_requesting()
    {
        // Arrange
        using var mux = new Multiplexer();
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
        using var mux = new Multiplexer();
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
        using var mux = new Multiplexer();
        mux.SetConnected();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act
        var act = () => mux.RequestAsync(
            102,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            cancellationToken: cts.Token
        );

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(act);
    }

    [Fact]
    public async Task should_serialize_same_message_type_given_two_requests_when_dispatching()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var sendOrder = new List<byte>();
        var firstTask = mux.RequestAsync(
            120,
            [0x1],
            async (_, token) =>
            {
                sendOrder.Add(0x1);
                await Task.Delay(50, token);
            },
            TimeSpan.FromSeconds(1)
        );

        await Task.Delay(10);

        var secondTask = mux.RequestAsync(
            120,
            [0x2],
            (_, _) =>
            {
                sendOrder.Add(0x2);
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(1)
        );

        await Task.Delay(60);

        // Act
        mux.Dispatch(120, [0xA]);
        await Task.Delay(25);
        mux.Dispatch(120, [0xB]);

        // Assert
        Assert.Equal([0x1, 0x2], sendOrder);
        Assert.Equal([0xA], await firstTask);
        Assert.Equal([0xB], await secondTask);
    }

    [Fact]
    public void should_dispatch_to_all_registered_handlers_given_notification_message()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var seen = new List<string>();
        using var first = mux.RegisterNotificationHandler(130, _ => seen.Add("first"));
        using var second = mux.RegisterNotificationHandler(130, _ => seen.Add("second"));

        // Act
        mux.Dispatch(130, [0x1]);

        // Assert
        Assert.Equal(["first", "second"], seen);
    }

    [Fact]
    public async Task should_cancel_inflight_request_given_disconnect_when_cancel_all_is_called()
    {
        // Arrange
        using var mux = new Multiplexer();
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

    [Fact]
    public async Task should_dispatch_to_next_request_given_first_request_canceled_when_same_message_type_is_inflight()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var firstCts = new CancellationTokenSource();
        var first = mux.RequestAsync(
            122,
            [0x1],
            static async (_, token) =>
            {
                await Task.Delay(100, token);
            },
            TimeSpan.FromSeconds(5),
            cancellationToken: firstCts.Token
        );

        var second = mux.RequestAsync(
            122,
            [0x2],
            (_, _) =>
            {
                secondStarted.TrySetResult();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(5)
        );

        // Act
        await firstCts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await secondStarted.Task;
        mux.Dispatch(122, [0xB]);

        // Assert
        Assert.Equal([0xB], await second);
    }

    [Fact]
    public async Task should_route_uncorrelated_response_to_first_request_given_later_correlated_request_with_same_message_type()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var firstSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var firstTask = mux.RequestAsync(
            130,
            [0x01],
            async (_, token) =>
            {
                firstSendStarted.TrySetResult();
                await Task.Delay(10, token);
            },
            TimeSpan.FromSeconds(1)
        );

        await firstSendStarted.Task;

        var secondTask = mux.RequestAsync(
            130,
            [0x02],
            (_, _) =>
            {
                secondSendStarted.TrySetResult();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(1),
            responseMatcher: response => response.Length > 0 && response.Span[0] == 2
        );

        await secondSendStarted.Task;

        // Act
        mux.Dispatch(130, [0xA]);
        mux.Dispatch(130, [2]);

        // Assert
        Assert.Equal([0xA], await firstTask);
        Assert.Equal([2], await secondTask);
    }

    [Fact]
    public async Task should_match_responses_to_correlated_requests_given_same_message_type()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var firstSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondSendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var firstTask = mux.RequestAsync(
            131,
            [0x01],
            async (_, token) =>
            {
                firstSendStarted.TrySetResult();
                await Task.Delay(10, token);
            },
            TimeSpan.FromSeconds(1),
            responseMatcher: response => response.Length > 0 && response.Span[0] == 1
        );

        await firstSendStarted.Task;

        var secondTask = mux.RequestAsync(
            131,
            [0x02],
            (_, _) =>
            {
                secondSendStarted.TrySetResult();
                return Task.CompletedTask;
            },
            TimeSpan.FromSeconds(1),
            responseMatcher: response => response.Length > 0 && response.Span[0] == 2
        );

        await secondSendStarted.Task;

        // Act
        mux.Dispatch(131, [2]);
        mux.Dispatch(131, [1]);

        // Assert
        Assert.Equal([1], await firstTask);
        Assert.Equal([2], await secondTask);
    }

    [Fact]
    public async Task should_deliver_unmatched_responses_to_notification_handlers_given_matching_by_correlation_fails()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var notifications = new List<byte[]>();
        using var _ = mux.RegisterNotificationHandler(132, payload =>
        {
            notifications.Add(payload.ToArray());
        });

        var request = mux.RequestAsync(
            132,
            [0x01],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            responseMatcher: payload => payload.Length > 0 && payload.Span[0] == 0x99
        );

        mux.Dispatch(132, [0x01]);

        await Task.Delay(15);
        Assert.NotNull(notifications);
        Assert.Single(notifications);
        Assert.Equal([0x01], notifications[0]);

        // Act
        mux.Dispatch(132, [0x99]);

        // Assert
        Assert.Equal([0x99], await request);
        Assert.Single(notifications);
    }

    [Fact]
    public async Task should_ignore_stale_response_when_disconnected_before_following_request()
    {
        // Arrange
        using var mux = new Multiplexer();
        mux.SetConnected();

        var staleRequest = mux.RequestAsync(
            140,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromMilliseconds(20)
        );

        await Assert.ThrowsAsync<RequestTimeoutException>(() => staleRequest);

        mux.SetDisconnected();
        mux.Dispatch(140, [0xAA]);

        mux.SetConnected();
        var nextRequest = mux.RequestAsync(
            140,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(1)
        );

        mux.Dispatch(140, [0xB]);

        // Assert
        Assert.Equal([0xB], await nextRequest);
    }
}
