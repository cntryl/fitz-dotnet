using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class NoticeClientTests
{
    [Fact]
    public async Task SubscribeAsync_ConfiguredEnumerableCapacity_IsEnforced()
    {
        Action<ReadOnlyMemory<byte>>? notifyHandler = null;
        using var notice = new NoticeClient(
            (_, _, _) => ValueTask.CompletedTask,
            (messageType, _, _) =>
            {
                using var response = new BinaryBufferWriter();
                response.WriteU8(0);
                if (messageType == MessageTypes.NoticeSubscribe)
                {
                    response.WriteU8(1);
                    response.WriteU64(55);
                }
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(response.Build());
            },
            (_, handler) =>
            {
                notifyHandler = handler;
                return new TestRegistration();
            },
            subscriptionBufferCapacity: 1);
        await using var subscription = await notice.SubscribeAsync("notice://prod/app/*");
        await using var enumerator = subscription.GetAsyncEnumerator();

        notifyHandler!(NoticeNotification(55, "first"));
        await Task.Delay(50);
        notifyHandler(NoticeNotification(55, "second"));

        await Assert.ThrowsAsync<SubscriptionBackpressureException>(() =>
            subscription.Completion.WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.True(await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<SubscriptionBackpressureException>(() =>
            enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task UnsubscribeAsync_WireFailure_LeavesEnumerableActiveAndRetryable()
    {
        Action<ReadOnlyMemory<byte>>? notifyHandler = null;
        var unsubscribeAttempts = 0;
        using var notice = new NoticeClient(
            (_, _, _) => ValueTask.CompletedTask,
            (messageType, _, _) =>
            {
                if (messageType == MessageTypes.NoticeUnsubscribe && Interlocked.Increment(ref unsubscribeAttempts) == 1)
                {
                    return ValueTask.FromException<ReadOnlyMemory<byte>>(new ConnectionException("unsubscribe failed"));
                }

                using var response = new BinaryBufferWriter();
                response.WriteU8(0);
                if (messageType == MessageTypes.NoticeSubscribe)
                {
                    response.WriteU8(1);
                    response.WriteU64(55);
                }
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(response.Build());
            },
            (_, handler) =>
            {
                notifyHandler = handler;
                return new TestRegistration();
            });
        var subscription = await notice.SubscribeAsync("notice://prod/app/*");
        await using var enumerator = subscription.GetAsyncEnumerator();

        await Assert.ThrowsAsync<ConnectionException>(() => subscription.UnsubscribeAsync().AsTask());
        notifyHandler!(NoticeNotification(55, "still-active"));

        Assert.True(await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal("still-active", System.Text.Encoding.UTF8.GetString(enumerator.Current.Body.Span));

        await subscription.UnsubscribeAsync();
        Assert.Equal(2, Volatile.Read(ref unsubscribeAttempts));
    }

    [Fact]
    public async Task ShouldPreserveDomainErrorGivenInvalidPatternWhenSubscribing()
    {
        // Arrange
        using var notice = new NoticeClient(
            (_, _, _) => Task.CompletedTask,
            (_, _, _) =>
            {
                using var response = new BinaryBufferWriter();
                response.WriteU8(1);
                response.WriteU32(FitzErrorCodes.NoticeInvalidPattern);
                response.WriteString("invalid pattern");
                return Task.FromResult(response.Build());
            },
            (_, _) => new TestRegistration());

        // Act
        var act = () => notice.SubscribeAsync("notice://prod/app/*");

        // Assert
        var error = await Assert.ThrowsAsync<NoticeException>(act);
        Assert.Equal(FitzErrorCodes.NoticeInvalidPattern, error.DomainCode);
    }

    static byte[] NoticeNotification(ulong subscriptionId, string body)
    {
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(subscriptionId);
        notification.WriteString("notice://prod/app/events");
        var bytes = System.Text.Encoding.UTF8.GetBytes(body);
        notification.WriteU32((uint)bytes.Length);
        notification.WriteBytes(bytes);
        return notification.Build();
    }

    [Fact]
    public async Task ShouldYieldNoticeGivenAsyncEnumerableSubscriptionWhenNotificationArrives()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        using var notice = new NoticeClient(
            (_, _, _) => Task.CompletedTask,
            (messageType, _, _) =>
            {
                using var response = new BinaryBufferWriter();
                response.WriteU8(0);
                if (messageType == MessageTypes.NoticeSubscribe)
                {
                    response.WriteU8(1);
                    response.WriteU64(55);
                }
                return Task.FromResult(response.Build());
            },
            (_, handler) =>
            {
                notifyHandler = handler;
                return new TestRegistration();
            });
        await using var subscription = await notice.SubscribeAsync("notice://prod/app/*");
        await using var enumerator = subscription.GetAsyncEnumerator();

        // Act
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(55);
        notification.WriteString("notice://prod/app/events");
        notification.WriteU32(5);
        notification.WriteBytes("hello"u8);
        notifyHandler!(notification.Build());

        // Assert
        Assert.True(await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.Equal("notice://prod/app/events", enumerator.Current.Route);
    }

    [Fact]
    public async Task ShouldEncodeRouteAndBodyGivenNoticePayloadWhenPublishing()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        using var notice = new NoticeClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;
            return Task.CompletedTask;
        });

        // Act
        await notice.PublishAsync("notice://prod/app/events", "hello"u8.ToArray());

        // Assert
        Assert.Equal(MessageTypes.NoticePublish, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("notice://prod/app/events", reader.ReadString());
        Assert.Equal((uint)5, reader.ReadU32());
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(5)));
    }

    [Fact]
    public async Task ShouldInvokeNoticeHandlerGivenNotificationWhenSubscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        NoticeMessage? received = null;
        CancellationToken seenCancellationToken = default;
        var receivedTcs = new TaskCompletionSource<NoticeMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var notice = new NoticeClient(
            (_, _, _) => Task.CompletedTask,
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                if (messageType == MessageTypes.NoticeSubscribe)
                {
                    writer.WriteU8(1);
                    writer.WriteU64(55);
                }
                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.NoticeNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        // Act
        var subscription = await notice.SubscribeAsync("notice://prod/app/*", (message, cancellationToken) =>
        {
            received = message;
            seenCancellationToken = cancellationToken;
            receivedTcs.TrySetResult(message);
            return ValueTask.CompletedTask;
        });
        const ulong subscriptionId = 55;

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        Assert.Equal("notice://prod/app/*", subscription.Pattern);
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(subscriptionId);
        notification.WriteString("notice://prod/app/events");
        notification.WriteU32(5);
        notification.WriteBytes("hello"u8);
        notifyHandler!(notification.Build());

        var msg = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.NotNull(msg);
        Assert.Same(received, msg);
        Assert.Equal(MessageTypes.NoticeSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("notice://prod/app/events", msg!.Route);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(msg.Body.Span));
        Assert.NotEqual(default, seenCancellationToken);
        Assert.False(seenCancellationToken.IsCancellationRequested);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("notice://prod/app/*", reader.ReadString());

        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task ShouldCancelNoticeHandlerTokenGivenSubscriptionDisposedWhileHandlerIsRunning()
    {
        Action<byte[]>? notifyHandler = null;

        using var notice = new NoticeClient(
            (_, _, _) => Task.CompletedTask,
            (messageType, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);

                if (messageType == MessageTypes.NoticeSubscribe)
                {
                    writer.WriteU8(1);
                    writer.WriteU64(55);
                }

                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.NoticeNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        var handlerStarted = new TaskCompletionSource<CancellationToken>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await notice.SubscribeAsync("notice://prod/app/*", async (_, cancellationToken) =>
        {
            handlerStarted.TrySetResult(cancellationToken);
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                handlerCanceled.TrySetResult();
            }
        });
        const ulong subscriptionId = 55;

        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(subscriptionId);
        notification.WriteString("notice://prod/app/events");
        notification.WriteU32(5);
        notification.WriteBytes("hello"u8);
        notifyHandler!(notification.Build());

        var seenCancellationToken = await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.NotEqual(default, seenCancellationToken);
        Assert.False(seenCancellationToken.IsCancellationRequested);

        await subscription.DisposeAsync();

        await handlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ShouldSkipQueuedNoticeMessagesGivenSubscriptionDisposedBeforeNextHandlerRuns()
    {
        Action<byte[]>? notifyHandler = null;

        using var notice = new NoticeClient(
            (_, _, _) => Task.CompletedTask,
            (messageType, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);

                if (messageType == MessageTypes.NoticeSubscribe)
                {
                    writer.WriteU8(1);
                    writer.WriteU64(55);
                }

                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.NoticeNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handledRoutes = new List<string>();
        var subscription = await notice.SubscribeAsync("notice://prod/app/*", async (message, _) =>
        {
            lock (handledRoutes)
            {
                handledRoutes.Add(message.Route);
            }

            if (message.Route == "notice://prod/app/first")
            {
                firstHandlerStarted.TrySetResult();
                await releaseFirstHandler.Task.WaitAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
            }
        });
        const ulong subscriptionId = 55;

        Assert.NotNull(notifyHandler);
        using var firstNotification = new BinaryBufferWriter();
        firstNotification.WriteU64(subscriptionId);
        firstNotification.WriteString("notice://prod/app/first");
        firstNotification.WriteU32(5);
        firstNotification.WriteBytes("first"u8);
        notifyHandler!(firstNotification.Build());

        await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        using var secondNotification = new BinaryBufferWriter();
        secondNotification.WriteU64(subscriptionId);
        secondNotification.WriteString("notice://prod/app/second");
        secondNotification.WriteU32(6);
        secondNotification.WriteBytes("second"u8);
        notifyHandler!(secondNotification.Build());

        await subscription.DisposeAsync();
        releaseFirstHandler.TrySetResult();

        await Task.Delay(100);

        lock (handledRoutes)
        {
            Assert.Equal(["notice://prod/app/first"], handledRoutes);
        }
    }

}
