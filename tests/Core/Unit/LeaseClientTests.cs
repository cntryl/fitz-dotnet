using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseClientTests
{
    [Fact]
    public async Task AcquireAsync_EncodesRequestAndReturnsLeaseHandle()
    {
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteU64(77);
            return Task.FromResult(writer.Build());
        });

        var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        Assert.Equal(MessageTypes.LeaseAcquire, seenMessageType);
        Assert.NotNull(seenPayload);
        Assert.Equal((ulong)77, lease.Token);
        Assert.Equal("lease://prod/app/lock", lease.Route);

        var reader = new BinaryBufferReader(seenPayload!);
        Assert.Equal("lease://prod/app/lock", reader.ReadString());
        Assert.Equal(string.Empty, reader.ReadString());
        Assert.Equal((ulong)30, reader.ReadU64());
    }

    [Fact]
    public async Task QueryAsync_DecodesHeldLease()
    {
        var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            Assert.Equal(MessageTypes.LeaseQuery, messageType);

            var request = new BinaryBufferReader(payload);
            Assert.Equal("lease://prod/app/lock", request.ReadString());

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            writer.WriteU8(1);
            writer.WriteString("worker-1");
            writer.WriteU64(18);
            return Task.FromResult(writer.Build());
        });

        var info = await leaseClient.QueryAsync("lease://prod/app/lock");

        Assert.True(info.IsHeld);
        Assert.Equal("worker-1", info.Owner);
        Assert.Equal((ulong)18, info.TtlRemainingSecs);
    }
}