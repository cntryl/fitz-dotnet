using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class QueueClientTests
{
    [Fact]
    public async Task should_return_message_id_given_success_response_when_enqueueing()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        using var queue = new QueueClient((messageType, payload, _) =>
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
    public async Task should_encode_visibility_delay_given_nonzero_delay_when_enqueueing()
    {
        byte[]? seenPayload = null;
        using var queue = new QueueClient((_, payload, _) =>
        {
            seenPayload = payload;
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(555);
            return Task.FromResult(writer.Build());
        });

        await queue.EnqueueAsync("queue://prod/app/tasks", "job-1"u8.ToArray(), delayMs: 2_000);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("queue://prod/app/tasks", reader.ReadString());
        Assert.Equal((uint)5, reader.ReadU32());
        Assert.Equal("job-1", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(5)));
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)2, reader.ReadU64());
        Assert.True(reader.IsEof);
    }

    [Fact]
    public async Task should_round_subsecond_visibility_delay_up_to_one_second_when_enqueueing()
    {
        // Arrange
        byte[]? seenPayload = null;
        using var queue = new QueueClient((_, payload, _) =>
        {
            seenPayload = payload;
            return Task.FromResult(new byte[] { 0 });
        });

        // Act
        await queue.EnqueueAsync("queue://prod/app/tasks", "job-1"u8.ToArray(), delayMs: 1);

        // Assert
        var reader = new BinaryBufferReader(seenPayload!);
        _ = reader.ReadString();
        _ = reader.ReadBytes((int)reader.ReadU32());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)1, reader.ReadU64());
    }

    [Fact]
    public async Task should_return_reserved_items_given_success_response_when_reserving()
    {
        // Arrange
        byte[]? seenPayload = null;
        using var queue = new QueueClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.QueueReserve, messageType);
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(1);
            writer.WriteString("queue://prod/app/tasks");
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
        Assert.Equal("job-1", System.Text.Encoding.UTF8.GetString(items[0].Body.Span));
        Assert.Equal((uint)1, items[0].Attempt);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("queue://prod/app/tasks", reader.ReadString());
        Assert.Equal((ulong)30, reader.ReadU64());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((uint)2, reader.ReadU32());
        Assert.Equal((byte)1, reader.ReadU8());
        Assert.Equal((ulong)10, reader.ReadU64());
        Assert.True(reader.IsEof);
    }

    [Fact]
    public async Task should_return_concrete_routes_given_wildcard_queue_reserve()
    {
        // Arrange
        using var queue = new QueueClient((messageType, _, _) =>
        {
            Assert.Equal(MessageTypes.QueueReserve, messageType);

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(1);
            writer.WriteString("queue://acme/cats/cat");
            writer.WriteU64(555);
            writer.WriteU64(777);
            writer.WriteU32(5);
            writer.WriteBytes("job-1"u8);
            return Task.FromResult(writer.Build());
        });

        // Act
        var items = await queue.ReserveAsync("queue://*/cats/*", 30);

        // Assert
        var item = Assert.Single(items);
        Assert.Equal("queue://acme/cats/cat", item.Route);
        Assert.Equal("job-1", System.Text.Encoding.UTF8.GetString(item.Body.Span));
    }

    [Fact]
    public async Task should_reject_wildcard_route_given_queue_reserve_response()
    {
        // Arrange
        using var queue = new QueueClient((_, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(1);
            writer.WriteString("queue://*/cats/*");
            writer.WriteU64(555);
            writer.WriteU64(777);
            writer.WriteU32(0);
            return Task.FromResult(writer.Build());
        });

        // Act
        var result = () => queue.ReserveAsync("queue://*/cats/*", 30);

        // Assert
        await Assert.ThrowsAsync<QueueException>(result);
    }

    [Fact]
    public async Task should_send_one_blocking_reserve_given_wait_seconds()
    {
        // Arrange
        var reserveCallCount = 0;
        using var queue = new QueueClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.QueueReserve, messageType);
            reserveCallCount++;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU32(0);

            return Task.FromResult(writer.Build());
        });

        // Act
        var items = await queue.ReserveAsync("queue://prod/app/tasks", 30, waitSeconds: 1);

        // Assert
        Assert.Equal(1, reserveCallCount);
        Assert.Empty(items);
    }

    [Fact]
    public async Task should_surface_broker_rejection_without_polling_downgrade_given_wait_seconds()
    {
        // Arrange
        var reserveCallCount = 0;
        using var queue = new QueueClient((_, _, _) =>
        {
            reserveCallCount++;
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(1);
            writer.WriteU32(4008);
            writer.WriteString("Trailing data after RESERVE request");
            return Task.FromResult(writer.Build());
        });

        // Act
        var result = () => queue.ReserveAsync("queue://prod/app/tasks", 30, waitSeconds: 1);

        // Assert
        var error = await Assert.ThrowsAsync<QueueException>(result);
        Assert.Equal("RESERVE_FAILED", error.Code);
        Assert.Equal((uint)4008, error.DomainCode);
        Assert.Equal(1, reserveCallCount);
    }

    [Fact]
    public async Task should_invoke_queue_handler_given_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        QueueAvailabilityEvent? received = null;
        CancellationToken seenCancellationToken = default;
        var receivedTcs = new TaskCompletionSource<QueueAvailabilityEvent>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var queue = new QueueClient(
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU64(555);
                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.QueueNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        // Act
        var subscription = await queue.SubscribeAsync("queue://prod/app/*", (evt, cancellationToken) =>
        {
            received = evt;
            seenCancellationToken = cancellationToken;
            receivedTcs.TrySetResult(evt);
            return ValueTask.CompletedTask;
        });
        const ulong subscriptionId = 555;

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        notification.WriteU64(subscriptionId);
        notification.WriteString("queue://prod/app/tasks");
        notification.WriteU64(9);
        notification.WriteU64(2);
        notification.WriteU64(1);
        notifyHandler!(notification.Build());

        var evt = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.NotNull(evt);
        Assert.Equal(MessageTypes.QueueSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("queue://prod/app/tasks", evt!.Route);
        Assert.Equal((ulong)9, evt.ReadyMessages);
        Assert.Equal((ulong)2, evt.DelayedMessages);
        Assert.Equal((ulong)1, evt.InflightMessages);
        Assert.Same(received, evt);
        Assert.NotEqual(default, seenCancellationToken);
        Assert.False(seenCancellationToken.IsCancellationRequested);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("queue://prod/app/*", reader.ReadString());

        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task should_encode_tokens_given_reserved_item_when_extending_and_completing()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();
        using var queue = new QueueClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.QueueReserve)
            {
                writer.WriteU32(1);
                writer.WriteString("queue://prod/app/tasks");
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

    [Fact]
    public async Task should_mark_reserved_item_as_closed_after_disconnect()
    {
        // Arrange
        Action? onDisconnect = null;
        var unsubscribeCount = 0;

        using var queue = new QueueClient(
            (messageType, payload, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU32(1);
                writer.WriteString("queue://prod/app/tasks");
                writer.WriteU64(555);
                writer.WriteU64(777);
                writer.WriteU32(5);
                writer.WriteBytes("job-1"u8);
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
            },
            registerNotificationHandler: null,
            registerOnDisconnect: disconnect =>
            {
                onDisconnect = disconnect;
                return new TestRegistration(() => unsubscribeCount++);
            });

        var items = await queue.ReserveAsync("queue://prod/app/tasks", 30);
        var item = Assert.Single(items);

        // Act
        onDisconnect?.Invoke();

        var ex = await Assert.ThrowsAsync<QueueException>(() => item.ExtendAsync(10));

        // Assert
        Assert.Equal("ITEM_CLOSED", ex.Code);
        Assert.Equal("Queue item is no longer valid after disconnect", ex.Message);
        Assert.Equal(1, unsubscribeCount);
    }

    [Fact]
    public async Task should_mark_reserved_item_as_closed_after_reconnect()
    {
        await using var firstTransport = new TestQueuedTransport();
        await using var secondTransport = new TestQueuedTransport();
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var reserveWriter = new BinaryBufferWriter();
                reserveWriter.WriteU8(0);
                reserveWriter.WriteU32(1);
                reserveWriter.WriteString("queue://prod/app/tasks");
                reserveWriter.WriteU64(555);
                reserveWriter.WriteU64(777);
                reserveWriter.WriteU32(5);
                reserveWriter.WriteBytes("job-1"u8);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.QueueReserve, reserveWriter.WrittenSpan));
            }
        };

        secondTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var authProbeWriter = new BinaryBufferWriter();
            authProbeWriter.WriteU8(0);
            secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            reconnected.TrySetResult();
        };

        var transportFactoryCalls = 0;
        Func<ITransport> transportFactory = () => transportFactoryCalls++ == 0 ? firstTransport : secondTransport;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        using var queue = new QueueClient(connection);

        await connection.ConnectAsync();
        var items = await queue.ReserveAsync("queue://prod/app/tasks", 30);
        var item = Assert.Single(items);

        firstTransport.QueueClosed();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var ex = await Assert.ThrowsAsync<QueueException>(() => item.ExtendAsync(10));

        Assert.Equal("ITEM_CLOSED", ex.Code);
        Assert.Equal("Queue item is no longer valid after disconnect", ex.Message);

        await connection.CloseAsync();
    }

    [Fact]
    public async Task should_restore_queue_subscription_after_reconnect()
    {
        await using var firstTransport = new TestQueuedTransport();
        await using var secondTransport = new TestQueuedTransport();
        var firstNotification = new TaskCompletionSource<QueueAvailabilityEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondNotification = new TaskCompletionSource<QueueAvailabilityEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var notificationCount = 0;

        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var subscribeWriter = new BinaryBufferWriter();
                subscribeWriter.WriteU8(0);
                subscribeWriter.WriteU64(555);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.QueueSubscribe, subscribeWriter.WrittenSpan));
            }
        };

        secondTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var subscribeWriter = new BinaryBufferWriter();
                subscribeWriter.WriteU8(0);
                subscribeWriter.WriteU64(777);
                secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.QueueSubscribe, subscribeWriter.WrittenSpan));

                _ = Task.Run(async () =>
                {
                    await Task.Delay(50);
                    using var notification = new BinaryBufferWriter();
                    notification.WriteU64(777);
                    notification.WriteString("queue://prod/app/tasks");
                    notification.WriteU64(9);
                    notification.WriteU64(2);
                    notification.WriteU64(1);
                    secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.QueueNotify, notification.WrittenSpan));
                });
            }
        };

        var transportFactoryCalls = 0;
        Func<ITransport> transportFactory = () => transportFactoryCalls++ == 0 ? firstTransport : secondTransport;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        using var queue = new QueueClient(connection);

        await connection.ConnectAsync();
        var subscription = await queue.SubscribeAsync("queue://prod/app/*", (evt, _) =>
        {
            var seen = Interlocked.Increment(ref notificationCount);
            if (seen == 1)
            {
                firstNotification.TrySetResult(evt);
            }
            else if (seen == 2)
            {
                secondNotification.TrySetResult(evt);
            }

            return ValueTask.CompletedTask;
        });

        using (var notification = new BinaryBufferWriter())
        {
            notification.WriteU64(555);
            notification.WriteString("queue://prod/app/tasks");
            notification.WriteU64(3);
            notification.WriteU64(2);
            notification.WriteU64(1);
            firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.QueueNotify, notification.WrittenSpan));
        }

        var initialEvent = await firstNotification.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal((ulong)3, initialEvent.ReadyMessages);

        firstTransport.QueueClosed();

        var restoredEvent = await secondNotification.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal("queue://prod/app/tasks", restoredEvent.Route);
        Assert.Equal((ulong)9, restoredEvent.ReadyMessages);

        await connection.CloseAsync();
    }
}
