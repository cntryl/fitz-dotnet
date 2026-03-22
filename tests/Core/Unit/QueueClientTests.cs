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

            using var writer = new BinaryBufferWriter();
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

    [Fact]
    public async Task should_return_reserved_items_given_success_response_when_reserving()
    {
        // Arrange
        byte[]? seenPayload = null;
        var queue = new QueueClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.QueueReserve, messageType);
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(1);
            writer.WriteU64(555);
            writer.WriteU64(777);
            writer.WriteU32(5);
            writer.WriteBytes("job-1"u8);
            return Task.FromResult(writer.Build());
        });

        // Act
        var items = await queue.ReserveAsync("queue://prod/app/tasks", 30, batchSize: 2, waitSeconds: 10);

        // Assert
        Assert.NotNull(seenPayload);
        Assert.Single(items);
        Assert.Equal("queue://prod/app/tasks", items[0].Route);
        Assert.Equal((ulong)555, items[0].Id);
        Assert.Equal((ulong)777, items[0].Token);
        Assert.Equal("job-1", System.Text.Encoding.UTF8.GetString(items[0].Body.Span));
        Assert.Equal((uint)1, items[0].Attempt);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("queue://prod/app/tasks", reader.ReadString());
        Assert.Equal((ulong)30, reader.ReadU64());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((uint)2, reader.ReadU32());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)10, reader.ReadU64());
    }

    [Fact]
    public async Task should_yield_availability_event_given_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var queue = new QueueClient(
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
                Assert.Equal(MessageTypes.QueueNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        // Act
        var task = Task.Run(async () =>
        {
            await foreach (var evt in queue.SubscribeAsync("queue://prod/*"))
            {
                return evt;
            }

            return null;
        });

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteString("queue://prod/app/tasks");
        notification.WriteU64(9);
        notifyHandler!(notification.Build());

        var evt = await task;

        // Assert
        Assert.NotNull(evt);
        Assert.Equal(MessageTypes.QueueSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("queue://prod/app/tasks", evt!.Route);
        Assert.Equal((ulong)9, evt.MessageCount);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("queue://prod/*", reader.ReadString());
    }

    [Fact]
    public async Task should_encode_tokens_given_reserved_item_when_extending_and_completing()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();
        var queue = new QueueClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.QueueReserve)
            {
                writer.WriteU32(1);
                writer.WriteU64(555);
                writer.WriteU64(777);
                writer.WriteU32(5);
                writer.WriteBytes("job-1"u8);
            }

            return Task.FromResult(writer.Build());
        });

        // Act
        var reserved = await queue.ReserveAsync("queue://prod/app/tasks", 30);
        Assert.Single(reserved);
        await reserved[0].ExtendAsync(45);
        await reserved[0].CompleteAsync();
        await reserved[0].CompleteWithTokenAsync(999);

        // Assert
        Assert.Equal(4, calls.Count);
        Assert.Equal(MessageTypes.QueueReserve, calls[0].MessageType);
        Assert.Equal(MessageTypes.QueueExtend, calls[1].MessageType);
        Assert.Equal(MessageTypes.QueueComplete, calls[2].MessageType);
        Assert.Equal(MessageTypes.QueueComplete, calls[3].MessageType);

        var extendReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal("queue://prod/app/tasks", extendReader.ReadString());
        Assert.Equal((ulong)555, extendReader.ReadU64());
        Assert.Equal((ulong)777, extendReader.ReadU64());
        Assert.Equal((ulong)45, extendReader.ReadU64());

        var completeReader = new BinaryBufferReader(calls[2].Payload);
        Assert.Equal("queue://prod/app/tasks", completeReader.ReadString());
        Assert.Equal((ulong)555, completeReader.ReadU64());
        Assert.Equal((ulong)777, completeReader.ReadU64());

        var completeWithTokenReader = new BinaryBufferReader(calls[3].Payload);
        Assert.Equal("queue://prod/app/tasks", completeWithTokenReader.ReadString());
        Assert.Equal((ulong)555, completeWithTokenReader.ReadU64());
        Assert.Equal((ulong)999, completeWithTokenReader.ReadU64());
    }
}
