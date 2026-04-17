using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class NoticeClientTests
{
    [Fact]
    public async Task should_encode_route_and_body_given_notice_payload_when_publishing()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var notice = new NoticeClient((messageType, payload, _) =>
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
    public async Task should_invoke_notice_handler_given_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        NoticeMessage? received = null;
        CancellationToken seenCancellationToken = default;
        var receivedTcs = new TaskCompletionSource<NoticeMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var notice = new NoticeClient(
            (_, _, _) => Task.CompletedTask,
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU8(1);
                writer.WriteU64(55);
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

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        Assert.Equal("notice://prod/app/*", subscription.Pattern);
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(subscription.SubscriptionId);
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
    public async Task should_throw_invalid_route_given_wildcard_when_publishing_notice()
    {
        // Arrange
        var notice = new NoticeClient((_, _, _) => Task.CompletedTask);

        // Act
        var ex = await Assert.ThrowsAsync<NoticeException>(async () =>
        {
            await notice.PublishAsync("notice://prod/app/*", "hello"u8.ToArray());
        });

        // Assert
        Assert.Equal("INVALID_ROUTE", ex.Code);
    }
}
