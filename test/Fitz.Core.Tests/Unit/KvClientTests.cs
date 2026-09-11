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
    public async Task ShouldSerializeOperationsGivenConcurrentCallsOnSameTransaction()
    {
        // Arrange
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestCount = 0;
        await using var transaction = new KvTransaction(RequestAsync, "kv://prod/app/data", 7);

        async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
            ushort messageType,
            ReadOnlyMemory<byte> payload,
            CancellationToken ct)
        {
            _ = messageType;
            _ = payload;
            if (Interlocked.Increment(ref requestCount) == 1)
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task.WaitAsync(ct);
            }

            return new byte[] { 0 };
        }

        // Act
        var first = transaction.PutAsync("one"u8.ToArray(), "1"u8.ToArray());
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        var second = transaction.PutAsync("two"u8.ToArray(), "2"u8.ToArray());

        // Assert
        Assert.Equal(1, Volatile.Read(ref requestCount));
        releaseFirst.TrySetResult();
        await Task.WhenAll(first, second);
        Assert.Equal(2, Volatile.Read(ref requestCount));
    }

    [Fact]
    public async Task ShouldRejectInvalidFoundFlagGivenMalformedResponseWhenGettingValue()
    {
        // Arrange
        using var kv = new KvClient((messageType, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.KvBegin)
            {
                writer.WriteU64(7);
            }
            else
            {
                writer.WriteU8(2);
            }

            return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
        });

        // Act
        var transaction = await kv.BeginAsync("kv://prod/app/data", KvDurability.Sync);


        // Assert
        var error = await Assert.ThrowsAsync<KvException>(() => transaction.GetAsync("key"u8.ToArray()));

        Assert.Equal("GET_INVALID_RESPONSE", error.Code);
    }

    [Theory]
    [InlineData(2, 0)]
    [InlineData(0, 260)]
    public async Task ShouldRejectUndefinedEnumGivenInvalidValueWhenBeginningTransaction(int mode, int durability)
    {
        // Arrange
        var requestCalled = false;

        // Act
        using var kv = new KvClient((_, _, _) =>
        {
            requestCalled = true;
            return Task.FromResult(Array.Empty<byte>());
        });


        // Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => kv.BeginAsync(
            "kv://prod/app/users",
            (Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability)durability,
            (Cntryl.Fitz.Abstractions.Domains.Kv.KvMode)mode));

        Assert.False(requestCalled);
    }

    [Fact]
    public async Task ShouldReleaseDisconnectRegistrationGivenCleanupFailureWhenDisposingTransaction()
    {
        // Arrange
        var registrations = 0;
        var transaction = new KvTransaction(
            (_, _, _) => ValueTask.FromException<ReadOnlyMemory<byte>>(new KvException("rollback failed", "ROLLBACK_FAILED")),
            "kv://prod/app/users",
            7,
            _ =>
            {
                registrations++;
                return new global::Cntryl.Fitz.Core.Tests.Unit.TestRegistration(() => registrations--);
            });


        // Act
        await transaction.DisposeAsync();


        // Assert
        Assert.Equal(0, registrations);
        var error = await Assert.ThrowsAsync<KvException>(() => transaction.GetAsync("key"u8.ToArray()));
        Assert.Equal("TX_CLOSED", error.Code);
    }

    [Fact]
    public async Task ShouldRejectImpossibleScanCountBeforeAllocatingGivenActiveTransactionWhenOperationRuns()
    {
        // Arrange
        using var kv = new KvClient((messageType, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (messageType == MessageTypes.KvBegin)
                writer.WriteU64(7);
            else if (messageType == MessageTypes.KvScan)
                writer.WriteU32(uint.MaxValue);
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
        });

        // Act
        var transaction = await kv.BeginAsync("kv://prod/app/data", KvDurability.Sync);


        // Assert
        var exception = await Assert.ThrowsAsync<KvException>(() =>
            transaction.ScanAsync(new KvScanQuery()));

        Assert.Equal("SCAN_INVALID_RESPONSE", exception.Code);
    }

    [Fact]
    public async Task ShouldAllowCommitRetryAfterRejectedWireOperationGivenActiveTransactionWhenOperationRuns()
    {
        // Arrange
        var commitCalls = 0;
        using var kv = new KvClient((messageType, _, _) =>
        {
            using var writer = new BinaryBufferWriter();
            if (messageType == MessageTypes.KvBegin)
            {
                writer.WriteU8(0);
                writer.WriteU64(7);
            }
            else if (Interlocked.Increment(ref commitCalls) == 1)
            {
                writer.WriteU8(1);
                writer.WriteU32(FitzErrorCodes.KvBackendError);
                writer.WriteString("try again");
            }
            else
            {
                writer.WriteU8(0);
            }

            return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
        });

        // Act
        var transaction = await kv.BeginAsync("kv://prod/app/data", KvDurability.Sync);


        // Assert
        await Assert.ThrowsAsync<KvException>(() => transaction.CommitAsync());
        await transaction.CommitAsync();
        var closed = await Assert.ThrowsAsync<KvException>(() => transaction.CommitAsync());

        Assert.Equal("TX_CLOSED", closed.Code);
        Assert.Equal(2, commitCalls);
    }

    [Fact]
    public async Task ShouldKeepTransactionRetryableGivenRejectedCommitWhenTransactionOperationRuns()
    {
        // Arrange
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

        // Act
        var transaction = await kv.BeginAsync(
            "kv://realm/area/resource",
            Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Sync);


        // Assert
        await Assert.ThrowsAsync<KvException>(() => transaction.CommitAsync());
        await transaction.DisposeAsync();
        var error = await Assert.ThrowsAsync<KvException>(
            () => transaction.GetAsync("key"u8.ToArray()));

        Assert.Equal("TX_CLOSED", error.Code);
        Assert.Equal(1, rollbackRequests);
    }

    [Fact]
    public async Task ShouldDeliverExactRouteGivenWildcardKvSubscriptionWhenNotificationArrives()
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
    public async Task ShouldPreserveDomainErrorGivenInvalidSubscriptionWhenSubscribing()
    {
        // Arrange
        using var kv = new KvClient(
            request: (_, _, _) =>
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(1);
                writer.WriteU32(FitzErrorCodes.KvInvalidSubscriptionPattern);
                writer.WriteString("invalid pattern");
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
            },
            registerNotificationHandler: (_, _) => new TestRegistration());

        // Act
        var error = await Assert.ThrowsAsync<KvException>(() =>
            kv.SubscribeAsync("kv://realm/area/resource", (_, _) => ValueTask.CompletedTask));

        // Assert
        Assert.Equal(FitzErrorCodes.KvInvalidSubscriptionPattern, error.DomainCode);
        Assert.Contains("invalid pattern", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ShouldThrowKvExceptionGivenTruncatedErrorWhenSubscribing()
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
    public async Task ShouldReturnTransactionGivenSuccessResponseWhenBeginningTransaction()
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
    public async Task ShouldReturnFoundValueGivenExistingKeyWhenGettingFromTransaction()
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
    public async Task ShouldReturnNotFoundGivenCanonicalEmptyValueWhenGettingMissingKey()
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
    public async Task ShouldInsertKeySuccessfullyGivenValidTransactionWhenCallingInsertAsync()
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
    public async Task ShouldDeleteKeySuccessfullyGivenValidTransactionWhenCallingDeleteAsync()
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
    public async Task ShouldDeleteRangeSuccessfullyGivenValidTransactionWhenCallingDeleteRangeAsync()
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
    public async Task ShouldScanKeysSuccessfullyGivenValidTransactionWhenCallingScanAsync()
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
    public async Task ShouldRejectEmptyRouteBeforeBeginningTransactionGivenActiveTransactionWhenOperationRuns()
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
    public async Task ShouldMarkTransactionAsClosedAfterReconnectGivenActiveTransactionWhenOperationRuns()
    {
        // Arrange
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

        // Act
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(1));


        // Assert
        var ex = await Assert.ThrowsAsync<KvException>(() => tx.GetAsync("user:1"u8.ToArray()));

        Assert.Equal("TX_CLOSED", ex.Code);
        Assert.Equal("Transaction is no longer valid after disconnect", ex.Message);

        await connection.CloseAsync();
    }

    [Fact]
    public async Task ShouldRejectUndefinedModeGivenOutOfRangeEnumWhenBeginCalled()
    {
        // Arrange
        var transportCalls = 0;
        using var kv = new KvClient((_, _, _) =>
        {
            transportCalls++;
            return Task.FromResult(Array.Empty<byte>());
        });

        // Act
        var error = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => kv.BeginAsync("kv://prod/app/data", KvDurability.Sync, (KvMode)99));

        // Assert
        Assert.Equal("mode", error.ParamName);
        Assert.Equal(0, transportCalls);
    }

    [Fact]
    public async Task ShouldRejectUndefinedDurabilityGivenOutOfRangeEnumWhenBeginCalled()
    {
        // Arrange
        var transportCalls = 0;
        using var kv = new KvClient((_, _, _) =>
        {
            transportCalls++;
            return Task.FromResult(Array.Empty<byte>());
        });

        // Act
        var error = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => kv.BeginAsync("kv://prod/app/data", (KvDurability)99));

        // Assert
        Assert.Equal("durability", error.ParamName);
        Assert.Equal(0, transportCalls);
    }

    sealed class TestRegistration : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
