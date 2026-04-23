using System.Threading.Channels;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SubscriptionDispatchTests
{
    [Fact]
    public async Task should_return_from_start_given_message_already_queued_when_pump_begins()
    {
        var channel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        using var registration = new SubscriptionRegistration<int>(channel);
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        channel.Writer.TryWrite(1);

        var startTask = Task.Run(() =>
            SubscriptionPump.Start(registration, async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }));

        await startTask.WaitAsync(TimeSpan.FromSeconds(1));
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        releaseHandler.TrySetResult();
    }

    [Fact]
    public async Task should_cancel_handler_token_given_registration_disposed_while_handler_is_running()
    {
        var channel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        using var registration = new SubscriptionRegistration<int>(channel);
        var handlerStarted = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        SubscriptionPump.Start(registration, async (_, cancellationToken) =>
        {
            handlerStarted.TrySetResult(cancellationToken);
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                handlerCanceled.TrySetResult();
            }
        });

        channel.Writer.TryWrite(1);

        var handlerToken = await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.NotEqual(default, handlerToken);
        Assert.False(handlerToken.IsCancellationRequested);

        registration.Dispose();

        await handlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task should_skip_queued_messages_given_registration_disposed_before_next_handler_runs()
    {
        var channel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
        });
        using var registration = new SubscriptionRegistration<int>(channel);
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handledMessages = new List<int>();

        SubscriptionPump.Start(registration, async (message, _) =>
        {
            lock (handledMessages)
            {
                handledMessages.Add(message);
            }

            if (message == 1)
            {
                firstHandlerStarted.TrySetResult();
                await releaseFirstHandler.Task.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }
        });

        channel.Writer.TryWrite(1);
        await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        channel.Writer.TryWrite(2);
        registration.Dispose();
        releaseFirstHandler.TrySetResult();

        await Task.Delay(100);

        lock (handledMessages)
        {
            Assert.Equal([1], handledMessages);
        }
    }
}