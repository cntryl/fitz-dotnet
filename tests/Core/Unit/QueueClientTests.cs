using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class QueueClientTests
{
    [Fact]
    public async Task should_return_message_id_given_success_response_when_enqueueing()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var queue = new QueueClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(555);
            return Task.FromResult(writer.Build());
        });

        // Act
        var id = await queue.EnqueueAsync("queue://prod/app/tasks", "job-1"u8.ToArray());

        // Assert
        Assert.Equal((ulong)555, id);
        Assert.Equal(MessageTypes.QueueEnqueue, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("queue://prod/app/tasks", reader.ReadString());
        Assert.Equal((uint)5, reader.ReadU32());
        Assert.Equal("job-1", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(5)));
    }
}