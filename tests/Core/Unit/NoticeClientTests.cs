using Cntryl.Fitz.Domains.Notice;
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
    public async Task should_yield_notice_message_given_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var notice = new NoticeClient(
            (_, _, _) => Task.CompletedTask,
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.NoticeNotify, messageType);
                notifyHandler = handler;
            },
            _ => { });

        // Act
        var task = Task.Run(async () =>
        {
            await foreach (var msg in notice.SubscribeAsync("notice://prod/*"))
            {
                return msg;
            }

            return null;
        });

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteString("notice://prod/app/events");
        notification.WriteU32(5);
        notification.WriteBytes("hello"u8);
        notifyHandler!(notification.Build());

        var msg = await task;

        // Assert
        Assert.NotNull(msg);
        Assert.Equal(MessageTypes.NoticeSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("notice://prod/app/events", msg!.Route);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(msg.Body.Span));

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("notice://prod/*", reader.ReadString());
    }
}