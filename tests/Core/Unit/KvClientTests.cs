using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class KvClientTests
{
    [Fact]
    public async Task should_close_transaction_without_rollback_given_rejected_commit()
    {
        var rollbackRequests = 0;
        using var kv = new KvClient((messageType, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(messageType == MessageTypes.KvCommit ? (byte)1 : (byte)0);
            if (messageType == MessageTypes.KvBegin)
            {
                writer.WriteU64(7);
            }
            if (messageType == MessageTypes.KvRollback)
            {
                rollbackRequests++;
            }
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
        });
        var transaction = await kv.BeginAsync(
            "kv://realm/area/resource",
            Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Sync);

        await Assert.ThrowsAsync<KvException>(() => transaction.CommitAsync());
        await transaction.DisposeAsync();
        var error = await Assert.ThrowsAsync<KvException>(
            () => transaction.GetAsync("key"u8.ToArray()));

        Assert.Equal("TX_CLOSED", error.Code);
        Assert.Equal(0, rollbackRequests);
    }

    [Fact]
    public async Task should_deliver_exact_route_given_wildcard_kv_subscription_when_notification_arrives()
    {
        // Arrange
        Action<ReadOnlyMemory<byte>>? notificationHandler = null;
        var received = new TaskCompletionSource<KvNotification>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var kv = new KvClient(
            request: (messageType, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                if (messageType == MessageTypes.KvSubscribe)
                {
                    writer.WriteU64(42);
                }
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
            },
            registerNotificationHandler: (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.KvNotify, messageType);
                notificationHandler = handler;
                return new TestRegistration();
            });

        // Act
        var subscription = await kv.SubscribeAsync("kv://*/area/**", (notification, _) =>
        {
            received.TrySetResult(notification);
            return ValueTask.CompletedTask;
        });
        using var payload = new BinaryBufferWriter();
        payload.WriteU64(42);
        payload.WriteString("kv://realm/area/resource");
        payload.WriteU64(3);
        notificationHandler!(payload.WrittenMemory);
        var notification = await received.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.Equal("kv://realm/area/resource", notification.Route);
        Assert.Equal((ulong)3, notification.MutationCount);
        await subscription.DisposeAsync();
    }

    [Fact]
    public async Task should_preserve_message_given_kv_subscription_validation_error()
    {
        // Arrange
        using var kv = new KvClient(
            request: (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(1);
                writer.WriteString("invalid pattern");
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
            },
            registerNotificationHandler: (_, _) => new TestRegistration());

        // Act
        var error = await Assert.ThrowsAsync<KvException>(() =>
            kv.SubscribeAsync("kv://realm/area/resource", (_, _) => ValueTask.CompletedTask));

        // Assert
        Assert.Null(error.DomainCode);
        Assert.Contains("invalid pattern", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task should_throw_kv_exception_given_truncated_subscription_error_response()
    {
        // Arrange
        using var kv = new KvClient(
            request: (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(1);
                writer.WriteU32(4);
                writer.WriteU8(1);
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
            },
            registerNotificationHandler: (_, _) => new TestRegistration());

        // Act
        var error = await Assert.ThrowsAsync<KvException>(() =>
            kv.SubscribeAsync("kv://realm/area/resource", (_, _) => ValueTask.CompletedTask));

        // Assert
        Assert.Equal("SUBSCRIBE_INVALID_RESPONSE", error.Code);
    }

    [Fact]
    public async Task should_return_transaction_given_success_response_when_beginning_transaction()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        using var kv = new KvClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(42);
            return Task.FromResult(writer.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async, KvMode.ReadWrite);

        // Assert
        Assert.NotNull(tx);
        Assert.Equal(MessageTypes.KvBegin, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("kv://prod/app/users", reader.ReadString());
        Assert.Equal((byte)KvMode.ReadWrite, reader.ReadU8());
        Assert.Equal((byte)Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async, reader.ReadU8());
    }

    [Fact]
    public async Task should_return_found_value_given_existing_key_when_getting_from_transaction()
    {
        // Arrange
        var callCount = 0;
        using var kv = new KvClient((messageType, payload, _) =>
        {
            callCount++;
            if (callCount == 1)
            {
                using var begin = new BinaryBufferWriter();
                begin.WriteU8(0);
                begin.WriteU64(900);
                return Task.FromResult(begin.Build());
            }

            Assert.Equal(MessageTypes.KvGet, messageType);
            using var get = new BinaryBufferWriter();
            get.WriteU8(0);
            get.WriteU8(1);
            get.WriteU32(5);
            get.WriteBytes("alice"u8);
            return Task.FromResult(get.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        var result = await tx.GetAsync(new ReadOnlyMemory<byte>("user:1"u8.ToArray()));

        // Assert
        Assert.True(result.Found);
        Assert.Equal("alice", System.Text.Encoding.UTF8.GetString(result.Value!.Value.Span));
    }

    [Fact]
    public async Task should_return_not_found_given_canonical_empty_value_when_getting_missing_key()
    {
        // Arrange
        var callCount = 0;
        using var kv = new KvClient((_, _, _) =>
        {
            callCount++;
            using var response = new BinaryBufferWriter();
            response.WriteU8(0);
            if (callCount == 1)
            {
                response.WriteU64(901);
            }
            else
            {
                response.WriteU8(0);
                response.WriteU32(0);
            }

            return Task.FromResult(response.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        var result = await tx.GetAsync("missing"u8.ToArray());

        // Assert
        Assert.False(result.Found);
        Assert.Null(result.Value);
    }

    [Fact]
    public async Task should_insert_key_successfully_when_calling_insert_async()
    {
        // Arrange
        var calls = new List<ushort>();
        using var kv = new KvClient((messageType, payload, _) =>
        {
            calls.Add(messageType);
            using var response = new BinaryBufferWriter();
            response.WriteU8(0);
            if (messageType == MessageTypes.KvBegin)
            {
                response.WriteU64(123);
            }
            return Task.FromResult(response.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        await tx.InsertAsync(new ReadOnlyMemory<byte>("user:2"u8.ToArray()), new ReadOnlyMemory<byte>("bob"u8.ToArray()));

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.KvBegin, calls[0]);
        Assert.Equal(MessageTypes.KvInsert, calls[1]);
    }

    [Fact]
    public async Task should_delete_key_successfully_when_calling_delete_async()
    {
        // Arrange
        var calls = new List<ushort>();
        using var kv = new KvClient((messageType, payload, _) =>
        {
            calls.Add(messageType);
            using var response = new BinaryBufferWriter();
            response.WriteU8(0);
            if (messageType == MessageTypes.KvBegin)
            {
                response.WriteU64(124);
            }
            return Task.FromResult(response.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        await tx.DeleteAsync(new ReadOnlyMemory<byte>("user:1"u8.ToArray()));

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.KvBegin, calls[0]);
        Assert.Equal(MessageTypes.KvDelete, calls[1]);
    }

    [Fact]
    public async Task should_delete_range_successfully_when_calling_delete_range_async()
    {
        // Arrange
        var calls = new List<ushort>();
        using var kv = new KvClient((messageType, payload, _) =>
        {
            calls.Add(messageType);
            using var response = new BinaryBufferWriter();
            response.WriteU8(0);
            if (messageType == MessageTypes.KvBegin)
            {
                response.WriteU64(125);
            }
            return Task.FromResult(response.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        await tx.DeleteRangeAsync(new ReadOnlyMemory<byte>("user:1"u8.ToArray()), new ReadOnlyMemory<byte>("user:9"u8.ToArray()));

        // Assert
        Assert.Equal(2, calls.Count);
        Assert.Equal(MessageTypes.KvBegin, calls[0]);
        Assert.Equal(MessageTypes.KvDeleteRange, calls[1]);
    }

    [Fact]
    public async Task should_scan_keys_successfully_when_calling_scan_async()
    {
        // Arrange
        ushort seenMessageType = 0;
        using var kv = new KvClient((messageType, payload, _) =>
        {
            if (seenMessageType == 0)
            {
                seenMessageType = messageType;
                using var begin = new BinaryBufferWriter();
                begin.WriteU8(0);
                begin.WriteU64(126);
                return Task.FromResult(begin.Build());
            }

            Assert.Equal(MessageTypes.KvScan, messageType);
            using var scan = new BinaryBufferWriter();
            scan.WriteU8(0); // status
            scan.WriteU32(2); // 2 key-value pairs

            // First pair
            scan.WriteU32(4);
            scan.WriteBytes("key1"u8.ToArray());
            scan.WriteU32(6);
            scan.WriteBytes("value1"u8.ToArray());

            // Second pair
            scan.WriteU32(4);
            scan.WriteBytes("key2"u8.ToArray());
            scan.WriteU32(6);
            scan.WriteBytes("value2"u8.ToArray());
            scan.WriteU8(1); // has_more

            return Task.FromResult(scan.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        var result = await tx.ScanAsync(new KvScanQuery());
        var pairs = result.Pairs;

        // Assert
        Assert.Equal(2, pairs.Count);
        Assert.Equal("key1", System.Text.Encoding.UTF8.GetString(pairs[0].Key.Span));
        Assert.Equal("value1", System.Text.Encoding.UTF8.GetString(pairs[0].Value.Span));
        Assert.True(result.HasMore);
    }

    [Fact]
    public async Task should_reject_empty_route_before_beginning_transaction()
    {
        // Arrange
        var requestCount = 0;
        using var kv = new KvClient((_, _, _) =>
        {
            requestCount++;
            return Task.FromResult(Array.Empty<byte>());
        });

        // Act
        var ex = await Assert.ThrowsAsync<KvException>(async () =>
        {
            await kv.BeginAsync("", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
        });

        // Assert
        Assert.Equal("INVALID_ROUTE", ex.Code);
        Assert.Contains("must be kv://{realm}/{area}/{resource}", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, requestCount);
    }

    [Fact]
    public async Task should_mark_transaction_as_closed_after_reconnect()
    {
        await using var firstTransport = new TestQueuedTransport();
        await using var secondTransport = new TestQueuedTransport();
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        firstTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount == 1)
            {
                using var authWriter = new BinaryBufferWriter();
                authWriter.WriteU8(0);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authWriter.WrittenSpan));
            }
            else if (sentFrameCount == 2)
            {
                using var beginWriter = new BinaryBufferWriter();
                beginWriter.WriteU8(0);
                beginWriter.WriteU64(77);
                firstTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.KvBegin, beginWriter.WrittenSpan));
            }
        };

        secondTransport.AfterSend = sentFrameCount =>
        {
            if (sentFrameCount != 1)
            {
                return;
            }

            using var authWriter = new BinaryBufferWriter();
            authWriter.WriteU8(0);
            secondTransport.QueueIncomingFrame(FrameCodec.Encode(MessageTypes.LeaseQuery, authWriter.WrittenSpan));
            reconnected.TrySetResult();
        };

        var transportFactoryCalls = 0;
        Func<ITransport> transportFactory = () => transportFactoryCalls++ == 0 ? firstTransport : secondTransport;
        await using var connection = new FitzConnection(
            new ClientConfig(
                new Uri("ws://localhost:4190/ws"),
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        using var kv = new KvClient(connection);

        await connection.ConnectAsync();
        var tx = await kv.BeginAsync("kv://prod/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);

        firstTransport.QueueClosed();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var ex = await Assert.ThrowsAsync<KvException>(() => tx.GetAsync("user:1"u8.ToArray()));

        Assert.Equal("TX_CLOSED", ex.Code);
        Assert.Equal("Transaction is no longer valid after disconnect", ex.Message);

        await connection.CloseAsync();
    }

    private sealed class TestRegistration : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
