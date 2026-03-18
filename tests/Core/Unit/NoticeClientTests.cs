using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class NoticeClientTests
{
    [Fact]
    public async Task PublishAsync_EncodesRouteAndBody()
    {
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var notice = new NoticeClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;
            return Task.CompletedTask;
        });

        await notice.PublishAsync("notice://prod/app/events", "hello"u8.ToArray());

        Assert.Equal(MessageTypes.NoticePublish, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("notice://prod/app/events", reader.ReadString());
        Assert.Equal((uint)5, reader.ReadU32());
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(5)));
    }
}