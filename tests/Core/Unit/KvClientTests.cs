using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Protocol;

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

            var writer = new BinaryBufferWriter();
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
                var begin = new BinaryBufferWriter();
                begin.WriteU8(0);
                begin.WriteU64(900);
                return Task.FromResult(begin.Build());
            }

            Assert.Equal(MessageTypes.KvGet, messageType);
            var get = new BinaryBufferWriter();
            get.WriteU8(0);
            get.WriteU8(1);
            get.WriteU32(5);
            get.WriteBytes("alice"u8);
            return Task.FromResult(get.Build());
        });

        // Act
        var tx = await kv.BeginAsync("kv://prod/app/users");
        var result = await tx.GetAsync("user:1"u8.ToArray());

        // Assert
        Assert.True(result.Found);
        Assert.Equal("alice", System.Text.Encoding.UTF8.GetString(result.Value!));
    }
}