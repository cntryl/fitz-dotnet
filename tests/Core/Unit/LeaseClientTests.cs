using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseClientTests
{
    [Fact]
    public async Task should_return_lease_handle_given_success_response_when_acquiring_lease()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(77);
            return Task.FromResult(writer.Build());
        });

        // Act
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Assert
        Assert.Equal(MessageTypes.LeaseAcquire, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("lease://prod/app/lock", lease.Route);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://prod/app/lock", reader.ReadString());
        Assert.Equal(string.Empty, reader.ReadString());
        Assert.Equal((ulong)30, reader.ReadU64());
    }

    [Fact]
    public async Task should_return_held_lease_info_given_holder_present_when_querying_lease()
    {
        // Arrange
        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.LeaseQuery, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("lease://prod/app/lock", request.ReadString());

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteString("worker-1");
            writer.WriteU64(18);
            return Task.FromResult(writer.Build());
        });

        // Act
        var info = await leaseClient.QueryAsync("lease://prod/app/lock");

        // Assert
        Assert.True(info.IsHeld);
        Assert.Equal("worker-1", info.Owner);
        Assert.Equal((ulong)18, info.TtlRemainingSecs);
    }

    [Fact]
    public async Task should_ignore_pending_waiters_given_success_response_when_querying_lease()
    {
        // Arrange
        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.LeaseQuery, messageType);

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteString("worker-1");
            writer.WriteU64(18);
            writer.WriteU32(3);
            return Task.FromResult(writer.Build());
        });

        // Act
        var info = await leaseClient.QueryAsync("lease://prod/app/lock");

        // Assert
        Assert.True(info.IsHeld);
        Assert.Equal("worker-1", info.Owner);
        Assert.Equal((ulong)18, info.TtlRemainingSecs);
    }

    [Fact]
    public async Task should_encode_ttl_given_lease_handle_when_extending_lease()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.LeaseAcquire)
            {
                writer.WriteU8(1);
                writer.WriteU64(77);
            }

            return Task.FromResult(writer.Build());
        });

        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Act
        await lease.ExtendAsync(45);

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.LeaseAcquire, calls[0].MessageType);
        Assert.Equal(MessageTypes.LeaseRenew, calls[1].MessageType);

        var extendReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal("lease://prod/app/lock", extendReader.ReadString());
        Assert.Equal(string.Empty, extendReader.ReadString());
        Assert.Equal((ulong)77, extendReader.ReadU64());
        Assert.Equal((ulong)45, extendReader.ReadU64());
    }

    [Fact]
    public async Task should_encode_token_given_lease_handle_when_releasing_lease()
    {
        // Arrange
        var calls = new List<(ushort MessageType, byte[] Payload)>();

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            calls.Add((messageType, payload));

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.LeaseAcquire)
            {
                writer.WriteU8(1);
                writer.WriteU64(77);
            }

            return Task.FromResult(writer.Build());
        });

        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        // Act
        await lease.ReleaseAsync();

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.LeaseAcquire, calls[0].MessageType);
        Assert.Equal(MessageTypes.LeaseRelease, calls[1].MessageType);

        var releaseReader = new BinaryBufferReader(calls[1].Payload);
        Assert.Equal("lease://prod/app/lock", releaseReader.ReadString());
        Assert.Equal(string.Empty, releaseReader.ReadString());
        Assert.Equal((ulong)77, releaseReader.ReadU64());
    }

    [Fact]
    public async Task should_invoke_lease_handler_given_notification_when_subscribing()
    {
        // Arrange
        Action<byte[]>? notifyHandler = null;
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        LeaseChangeEvent? received = null;
        CancellationToken seenCancellationToken = default;
        var receivedTcs = new TaskCompletionSource<LeaseChangeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);

        var leaseClient = new LeaseClient(
            (messageType, payload, _) =>
            {
                seenMessageType = messageType;
                seenPayload = payload;

                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU8(1);
                writer.WriteU64(555);
                return Task.FromResult(writer.Build());
            },
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.LeaseNotify, messageType);
                notifyHandler = handler;
                return new TestRegistration();
            });

        // Act
        var subscription = await leaseClient.SubscribeAsync("lease://prod/app/lock", (evt, cancellationToken) =>
        {
            received = evt;
            seenCancellationToken = cancellationToken;
            receivedTcs.TrySetResult(evt);
            return ValueTask.CompletedTask;
        });

        await Task.Delay(25);
        Assert.NotNull(notifyHandler);
        using var notification = new BinaryBufferWriter();
        const ulong subscriptionId = 555;
        notification.WriteU64(subscriptionId);
        notification.WriteString("lease://prod/app/lock");
        notifyHandler!(notification.Build());

        var evt = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.NotNull(evt);
        Assert.Equal(MessageTypes.LeaseSubscribe, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal("lease://prod/app/lock", evt!.Route);
        Assert.Same(received, evt);
        Assert.NotEqual(default, seenCancellationToken);
        Assert.False(seenCancellationToken.IsCancellationRequested);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://prod/app/lock", reader.ReadString());

        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task should_forward_wildcard_route_without_local_validation_when_acquiring_lease()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(77);
            return Task.FromResult(writer.Build());
        });

        // Act
        var lease = await leaseClient.AcquireAsync("lease://prod/app/*", 30);

        // Assert
        Assert.Equal("lease://prod/app/*", lease.Route);
        Assert.Equal(MessageTypes.LeaseAcquire, seenMessageType);
        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://prod/app/*", reader.ReadString());
    }

    [Fact]
    public async Task should_mark_lease_as_closed_after_disconnect()
    {
        // Arrange
        var transport = new TestQueuedTransport();
        transport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authProbeWriter = new BinaryBufferWriter();
                authProbeWriter.WriteU8(0);
                transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authProbeWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var acquireWriter = new BinaryBufferWriter();
                acquireWriter.WriteU8(0);
                acquireWriter.WriteU8(1);
                acquireWriter.WriteU64(77);
                transport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseAcquire, acquireWriter.WrittenSpan));
            }
        };

        var config = new ClientConfig("ws://localhost:4190/ws", TransportFactory: _ => transport);
        var connection = new FitzConnection(config, () => transport);
        var leaseClient = new LeaseClient(connection);

        await connection.ConnectAsync();
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        await connection.CloseAsync();

        var ex = await Assert.ThrowsAsync<LeaseException>(() => lease.ExtendAsync(60));

        // Assert
        Assert.Equal("CLOSED", ex.Code);
        Assert.Equal("Lease handle is no longer valid after disconnect", ex.Message);
    }

    [Fact]
    public async Task should_mark_lease_as_closed_after_reconnect()
    {
        var firstTransport = new TestQueuedTransport();
        var secondTransport = new TestQueuedTransport();
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
                using var acquireWriter = new BinaryBufferWriter();
                acquireWriter.WriteU8(0);
                acquireWriter.WriteU8(1);
                acquireWriter.WriteU64(77);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseAcquire, acquireWriter.WrittenSpan));
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
        var connection = new FitzConnection(
            new ClientConfig(
                "ws://localhost:4190/ws",
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        var leaseClient = new LeaseClient(connection);

        await connection.ConnectAsync();
        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        firstTransport.QueueClosed();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var ex = await Assert.ThrowsAsync<LeaseException>(() => lease.ExtendAsync(60));

        Assert.Equal("CLOSED", ex.Code);
        Assert.Equal("Lease handle is no longer valid after disconnect", ex.Message);

        await connection.CloseAsync();
    }

    [Fact]
    public async Task should_restore_lease_subscription_after_reconnect()
    {
        var firstTransport = new TestQueuedTransport();
        var secondTransport = new TestQueuedTransport();
        var firstNotification = new TaskCompletionSource<LeaseChangeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondNotification = new TaskCompletionSource<LeaseChangeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                subscribeWriter.WriteU8(1);
                subscribeWriter.WriteU64(555);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseSubscribe, subscribeWriter.WrittenSpan));
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
                subscribeWriter.WriteU8(1);
                subscribeWriter.WriteU64(777);
                secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseSubscribe, subscribeWriter.WrittenSpan));

                _ = Task.Run(async () =>
                {
                    await Task.Delay(50).ConfigureAwait(false);
                    using var notification = new BinaryBufferWriter();
                    notification.WriteU64(777);
                    notification.WriteString("lease://prod/app/lock");
                    secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseNotify, notification.WrittenSpan));
                });
            }
        };

        var transportFactoryCalls = 0;
        Func<ITransport> transportFactory = () => transportFactoryCalls++ == 0 ? firstTransport : secondTransport;
        var connection = new FitzConnection(
            new ClientConfig(
                "ws://localhost:4190/ws",
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        var leaseClient = new LeaseClient(connection);

        await connection.ConnectAsync();
        var subscription = await leaseClient.SubscribeAsync("lease://prod/app/lock", (evt, _) =>
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
            notification.WriteString("lease://prod/app/lock");
            firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseNotify, notification.WrittenSpan));
        }

        var initialEvent = await firstNotification.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal("lease://prod/app/lock", initialEvent.Route);

        firstTransport.QueueClosed();

        var restoredEvent = await secondNotification.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal("lease://prod/app/lock", restoredEvent.Route);

        await connection.CloseAsync();
    }
}
