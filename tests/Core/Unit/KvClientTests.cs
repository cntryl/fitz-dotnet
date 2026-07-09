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
    public async Task should_return_transaction_given_success_response_when_beginning_transaction()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var kv = new KvClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(42);
            return Task.FromResult(writer.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users", KvMode.ReadWrite, KvDurability.Async);

        // Assert
        Assert.NotNull(tx);
        Assert.Equal(MessageTypes.KvBegin, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("kv://prod/app/users", reader.ReadString());
        Assert.Equal((byte)KvMode.ReadWrite, reader.ReadU8());
        Assert.Equal((byte)KvDurability.Async, reader.ReadU8());
    }

    [Fact]
    public async Task should_return_found_value_given_existing_key_when_getting_from_transaction()
    {
        // Arrange
        var callCount = 0;
        var kv = new KvClient((messageType, payload, _) =>
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
        var tx = await kv.BeginAsync("kv://prod/app/users");
        var result = await tx.GetAsync(new ReadOnlyMemory<byte>("user:1"u8.ToArray()));

        // Assert
        Assert.True(result.Found);
        Assert.Equal("alice", System.Text.Encoding.UTF8.GetString(result.Value!.Value.Span));
    }

    [Fact]
    public async Task should_insert_key_successfully_when_calling_insert_async()
    {
        // Arrange
        var calls = new List<ushort>();
        var kv = new KvClient((messageType, payload, _) =>
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
        var tx = await kv.BeginAsync("kv://prod/app/users");
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
        var kv = new KvClient((messageType, payload, _) =>
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
        var tx = await kv.BeginAsync("kv://prod/app/users");
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
        var kv = new KvClient((messageType, payload, _) =>
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
        var tx = await kv.BeginAsync("kv://prod/app/users");
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
        var kv = new KvClient((messageType, payload, _) =>
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

            return Task.FromResult(scan.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users");
        var enumerable = tx.ScanAsync(new KvScanQuery());
        var pairs = new List<KvPair>();
        await foreach (var pair in enumerable)
        {
            pairs.Add(pair);
        }

        // Assert
        Assert.Equal(2, pairs.Count);
        Assert.Equal("key1", System.Text.Encoding.UTF8.GetString(pairs[0].Key.Span));
        Assert.Equal("value1", System.Text.Encoding.UTF8.GetString(pairs[0].Value.Span));
    }

    [Fact]
    public async Task should_forward_wildcard_route_without_local_validation_when_beginning_transaction()
    {
        // Arrange
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;
        var kv = new KvClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU64(42);
            return Task.FromResult(writer.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/*/*");

        // Assert
        Assert.NotNull(tx);
        Assert.Equal(MessageTypes.KvBegin, seenMessageType);
        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("kv://prod/*/*", reader.ReadString());
    }

    [Fact]
    public async Task should_reject_empty_route_before_beginning_transaction()
    {
        // Arrange
        var requestCount = 0;
        var kv = new KvClient((_, _, _) =>
        {
            requestCount++;
            return Task.FromResult(Array.Empty<byte>());
        });

        // Act
        var ex = await Assert.ThrowsAsync<KvException>(async () =>
        {
            await kv.BeginAsync("");
        });

        // Assert
        Assert.Equal("INVALID_ROUTE", ex.Code);
        Assert.Contains("must be kv://{realm}/{area}/{resource}", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, requestCount);
    }

    [Fact]
    public async Task should_mark_transaction_as_closed_after_reconnect()
    {
        var firstTransport = new TestQueuedTransport();
        var secondTransport = new TestQueuedTransport();
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
        var connection = new FitzConnection(
            new ClientConfig(
                "ws://localhost:4190/ws",
                Reconnect: new ReconnectOptions(true, MaxAttempts: 1, Backoff: TimeSpan.FromMilliseconds(10), MaxBackoff: TimeSpan.FromMilliseconds(10))),
            transportFactory);
        var kv = new KvClient(connection);

        await connection.ConnectAsync();
        var tx = await kv.BeginAsync("kv://prod/app/users");

        firstTransport.QueueClosed();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var ex = await Assert.ThrowsAsync<KvException>(() => tx.GetAsync("user:1"u8.ToArray()));

        Assert.Equal("TX_CLOSED", ex.Code);
        Assert.Equal("Transaction is no longer valid after disconnect", ex.Message);

        await connection.CloseAsync();
    }
}
