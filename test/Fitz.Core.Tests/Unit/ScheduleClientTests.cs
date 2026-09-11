using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ScheduleClientTests
{
    [Fact]
    public async Task ShouldReturnTypedErrorGivenTruncatedSuccessWhenListingSchedules()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) => Task.FromResult(new byte[] { 0 }));

        var error = await Assert.ThrowsAsync<ScheduleException>(() => schedule.ListAsync());

        Assert.Equal("LIST_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task ShouldReturnTypedErrorGivenEmptyResponseWhenCancelingSchedule()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) => Task.FromResult(Array.Empty<byte>()));

        var error = await Assert.ThrowsAsync<ScheduleException>(() =>
            schedule.CancelAsync("schedule://prod/app/jobs/run"));

        Assert.Equal("CANCEL_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task ShouldRejectInvalidCreatedRouteFlagGivenMalformedResponseWhenCreatingSchedule()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) =>
            Task.FromResult(new byte[] { 0, 2 }));

        var error = await Assert.ThrowsAsync<ScheduleException>(() => schedule.CreateAsync(
            "schedule://prod/app/jobs/run",
            "*/5 * * * *",
            ScheduleDeliveryMode.Single,
            ReadOnlyMemory<byte>.Empty));

        Assert.Equal("CREATE_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task ShouldReturnTypedErrorGivenStatusOnlyFailureWhenListingSchedules()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) =>
            Task.FromResult(new byte[] { 1 }));

        var error = await Assert.ThrowsAsync<ScheduleException>(() => schedule.ListAsync());

        Assert.Equal("LIST_FAILED", error.Code);
        Assert.Equal((byte)1, error.Status);
        Assert.Null(error.DomainCode);
    }

    [Fact]
    public async Task ShouldRejectTrailingBytesGivenErrorResponseWhenListingSchedules()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) =>
            Task.FromResult(new byte[] { 1, 42 }));

        var error = await Assert.ThrowsAsync<ScheduleException>(() => schedule.ListAsync());

        Assert.Equal("LIST_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task ShouldPreserveBackendErrorCodeGivenCodedScheduleListFailureWhenScheduleOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
            writer.WriteU32(FitzErrorCodes.ScheduleBackendError);
            writer.WriteString("backend busy");
            return Task.FromResult(writer.Build());
        });

        var error = await Assert.ThrowsAsync<ScheduleException>(() => schedule.ListAsync());

        Assert.Equal(7010u, error.DomainCode);
        Assert.Contains("backend busy", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldPreserveMessageGivenScheduleErrorResponseWhenScheduleOperationRuns()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
            writer.WriteString("invalid delivery mode");
            return Task.FromResult(writer.Build());
        });

        var error = await Assert.ThrowsAsync<ScheduleException>(async () =>
            await schedule.CreateAsync("schedule://prod/app/jobs/run", "*/5 * * * *", ScheduleDeliveryMode.Single, ReadOnlyMemory<byte>.Empty));

        Assert.Null(error.DomainCode);
        Assert.Contains("invalid delivery mode", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldRejectUnauthorizedCreateGivenReadOnlyPermissionsWhenCreateCalled()
    {
        // Arrange
        // Act
        // Assert
        using var schedule = new ScheduleClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
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
        Assert.Null(error.DomainCode);
    }

    [Fact]
    public async Task ShouldReturnScheduleIdGivenSuccessResponseWhenCreatingSchedule()
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
    public async Task ShouldEncodeRouteGivenScheduleRouteWhenCancelingSchedule()
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
    public async Task ShouldInvokeScheduleHandlerGivenNotificationWhenSubscribing()
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
                if (messageType == MessageTypes.ScheduleUnsubscribe)
                {
                    return Task.FromResult(writer.Build());
                }
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
    public async Task ShouldReturnEntriesAndTotalCountGivenCanonicalListResponseWhenScheduleOperationRuns()
    {
        // Arrange
        byte[]? seenPayload = null;
        using var schedule = new ScheduleClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.ScheduleListPage, messageType);
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(12);
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
        var page = await schedule.ListAsync(offset: 0, limit: 25);

        // Assert
        Assert.Equal((ulong)12, page.TotalCount);
        Assert.Single(page.Entries);
        Assert.Null(page.Entries[0].Id);
        Assert.Equal("schedule://prod/app/jobs/run", page.Entries[0].Route);
        Assert.Equal("*/5 * * * *", page.Entries[0].Cron);
        Assert.Equal(ScheduleDeliveryMode.Single, page.Entries[0].DeliveryMode);
        Assert.Equal("job", System.Text.Encoding.UTF8.GetString(page.Entries[0].Payload));

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)0, reader.ReadU64());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)25, reader.ReadU64());
    }

    [Fact]
    public async Task ShouldRejectInvalidDeliveryModeGivenMalformedListResponseWhenScheduleOperationRuns()
    {
        // Arrange
        using var schedule = new ScheduleClient((messageType, _, _) =>
        {
            Assert.Equal(MessageTypes.ScheduleListPage, messageType);

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(1);
            writer.WriteU8(1);
            writer.WriteString("schedule://prod/app/jobs/run");
            writer.WriteString("*/5 * * * *");
            writer.WriteU8(2);
            writer.WriteU32(0);
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        var error = await Assert.ThrowsAsync<ScheduleException>(() => schedule.ListAsync());

        // Assert
        Assert.Equal("LIST_INVALID_RESPONSE", error.Code);
        Assert.Contains("invalid delivery mode 2", error.Message, StringComparison.Ordinal);
    }
}
