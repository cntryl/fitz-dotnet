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

            var writer = new BinaryBufferWriter();
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

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        await schedule.CancelAsync("schedule://prod/app/jobs");

        // Assert
    }
}