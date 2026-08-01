using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ScheduleClientTests
{
    [Fact]
    public async Task should_preserve_domain_code_given_schedule_error_response()
    {
        using var schedule = new ScheduleClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
            writer.WriteU32(7008);
            writer.WriteString("invalid delivery mode");
            return Task.FromResult(writer.Build());
        });

        var error = await Assert.ThrowsAsync<ScheduleException>(async () =>
            await schedule.CreateAsync("schedule://prod/app/jobs/run", "*/5 * * * *", ScheduleDeliveryMode.Single, ReadOnlyMemory<byte>.Empty));

        Assert.Equal((uint)7008, error.DomainCode);
    }

    [Fact]
    public async Task should_reject_unauthorized_create_given_read_only_permissions_when_create_called()
    {
        using var schedule = new ScheduleClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
            writer.WriteU32(7003);
            writer.WriteString("unauthorized");
            return Task.FromResult(writer.Build());
        });

        var error = await Assert.ThrowsAsync<ScheduleException>(async () =>
            await schedule.CreateAsync(
                "schedule://prod/app/jobs/run",
                "*/5 * * * *",
                ScheduleDeliveryMode.Broadcast,
                ReadOnlyMemory<byte>.Empty));

        Assert.Equal("CREATE failed: unauthorized", error.Message);
        Assert.Equal((uint)7003, error.DomainCode);
    }

    [Fact]
    public async Task should_return_schedule_id_given_success_response_when_creating_schedule()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        using var schedule = new ScheduleClient((messageType, payload, _) =>
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
        var id = await schedule.CreateAsync("schedule://prod/app/jobs/run", "*/5 * * * *", ScheduleDeliveryMode.Single, "job"u8.ToArray());

        // Assert
        Assert.Equal("sched-123", id);
        Assert.Equal(MessageTypes.ScheduleCreate, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("schedule://prod/app/jobs/run", reader.ReadString());
        Assert.Equal("*/5 * * * *", reader.ReadString());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((uint)3, reader.ReadU32());
        Assert.Equal("job", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(3)));
    }

    [Fact]
    public async Task should_encode_route_given_schedule_route_when_canceling_schedule()
    {
        // Arrange
        using var schedule = new ScheduleClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.ScheduleCancel, messageType);

            var reader = new BinaryBufferReader(payload);
            Assert.Equal("schedule://prod/app/jobs/run", reader.ReadString());

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        await schedule.CancelAsync("schedule://prod/app/jobs/run");

        // Assert
    }

    [Fact]
    public async Task should_invoke_schedule_handler_given_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        ScheduleNotification? received = null;
        CancellationToken seenCancellationToken = default;
        var receivedTcs = new TaskCompletionSource<ScheduleNotification>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var schedule = new ScheduleClient(
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
                Assert.Equal(MessageTypes.ScheduleNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        // Act
        var subscription = await schedule.SubscribeAsync("schedule://prod/app/jobs/run", (notification, cancellationToken) =>
        {
            received = notification;
            seenCancellationToken = cancellationToken;
            receivedTcs.TrySetResult(notification);
            return ValueTask.CompletedTask;
        });
        const ulong subscriptionId = 55;

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(subscriptionId);
        notification.WriteString("schedule://prod/app/jobs/run");
        notification.WriteU32(4);
        notification.WriteBytes("fire"u8);
        notifyHandler!(notification.Build());

        var evt = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.NotNull(evt);
        Assert.Equal(MessageTypes.ScheduleSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("schedule://prod/app/jobs/run", evt!.Route);
        Assert.Equal("fire", System.Text.Encoding.UTF8.GetString(evt!.Payload.Span));
        Assert.Same(received, evt);
        Assert.NotEqual(default, seenCancellationToken);
        Assert.False(seenCancellationToken.IsCancellationRequested);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("schedule://prod/app/jobs/run", reader.ReadString());

        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task should_return_entries_and_total_count_given_list_response_when_listing_schedules()
    {
        // Arrange
        byte[]? seenPayload = null;
        using var schedule = new ScheduleClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.ScheduleList, messageType);
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(2);
            writer.WriteU8(1);
            writer.WriteString("schedule://prod/app/jobs/run");
            writer.WriteString("*/5 * * * *");
            writer.WriteU8((byte)ScheduleDeliveryMode.Single);
            writer.WriteU32(3);
            writer.WriteBytes("job"u8);
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        var (entries, totalCount) = await schedule.ListAsync(offset: 10, limit: 25);

        // Assert
        Assert.Equal((ulong)2, totalCount);
        Assert.Single(entries);
        Assert.Equal("schedule://prod/app/jobs/run", entries[0].Route);
        Assert.Equal("*/5 * * * *", entries[0].Cron);
        Assert.Equal(ScheduleDeliveryMode.Single, entries[0].DeliveryMode);
        Assert.Equal("job", System.Text.Encoding.UTF8.GetString(entries[0].Payload));

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)10, reader.ReadU64());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)25, reader.ReadU64());
    }
}
