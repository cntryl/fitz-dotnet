using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ScheduleClientTests
{
    [Fact]
    public async Task should_return_schedule_id_given_success_response_when_creating_schedule()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var schedule = new ScheduleClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteString("sched-123");
            return Task.FromResult(writer.Build());
        });

        // Act
        var id = await schedule.CreateAsync("schedule://prod/app/jobs", "*/5 * * * *", "job"u8.ToArray());

        // Assert
        Assert.Equal("sched-123", id);
        Assert.Equal(MessageTypes.ScheduleCreate, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("schedule://prod/app/jobs", reader.ReadString());
        Assert.Equal("*/5 * * * *", reader.ReadString());
        Assert.Equal((uint)3, reader.ReadU32());
        Assert.Equal("job", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(3)));
    }

    [Fact]
    public async Task should_encode_route_given_schedule_route_when_canceling_schedule()
    {
        // Arrange
        var schedule = new ScheduleClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.ScheduleCancel, messageType);

            var reader = new BinaryBufferReader(payload);
            Assert.Equal("schedule://prod/app/jobs", reader.ReadString());

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        await schedule.CancelAsync("schedule://prod/app/jobs");

        // Assert
    }

    [Fact]
    public async Task should_yield_execution_event_given_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var schedule = new ScheduleClient(
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
                Assert.Equal(MessageTypes.ScheduleNotify, messageType);
                notifyHandler = handler;
            },
            _ => { });

        // Act
        var task = Task.Run(async () =>
        {
            await foreach (var evt in schedule.SubscribeAsync("schedule://prod/*"))
            {
                return evt;
            }

            return null;
        });

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteString("sched-1");
        notification.WriteString("schedule://prod/app/jobs");
        notification.WriteU64(123456UL);
        notifyHandler!(notification.Build());

        var evt = await task;

        // Assert
        Assert.NotNull(evt);
        Assert.Equal(MessageTypes.ScheduleSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("sched-1", evt!.ScheduleId);
        Assert.Equal("schedule://prod/app/jobs", evt.Route);
        Assert.Equal((ulong)123456, evt.ExecutedAtMs);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("schedule://prod/*", reader.ReadString());
    }
}