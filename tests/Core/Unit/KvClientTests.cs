using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class KvClientTests
{
    [Fact]
    public async Task BeginAsync_EncodesRequestAndReturnsTransaction()
    {
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

        var tx = await kv.BeginAsync("kv://prod/app/users", KvMode.ReadWrite, KvDurability.Async);

        Assert.NotNull(tx);
        Assert.Equal(MessageTypes.KvBegin, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("kv://prod/app/users", reader.ReadString());
        Assert.Equal((byte)KvMode.ReadWrite, reader.ReadU8());
        Assert.Equal((byte)KvDurability.Async, reader.ReadU8());
    }

    [Fact]
    public async Task TransactionGetAsync_DecodesFoundValue()
    {
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

        var tx = await kv.BeginAsync("kv://prod/app/users");
        var result = await tx.GetAsync("user:1"u8.ToArray());

        Assert.True(result.Found);
        Assert.Equal("alice", System.Text.Encoding.UTF8.GetString(result.Value!));
    }
}