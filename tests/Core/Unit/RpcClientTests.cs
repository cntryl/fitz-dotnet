using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcClientTests
{
    [Fact]
    public async Task RequestAsync_EncodesRequestAndAcceptsSuccessStatus()
    {
        ushort seenMessageType = 0;
        byte[]? seenPayload = null;

        var rpc = new RpcClient((messageType, payload, _) =>
        {
            seenMessageType = messageType;
            seenPayload = payload;

            var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            return Task.FromResult(writer.Build());
        });

        await rpc.RequestAsync("rpc://prod/app/echo", "ping"u8.ToArray());

        Assert.Equal(MessageTypes.RpcRequest, seenMessageType);
        Assert.NotNull(seenPayload);

        var reader = new BinaryBufferReader(seenPayload!);
        var corrLen = reader.ReadU32();
        Assert.Equal((uint)16, corrLen);
        _ = reader.ReadBytes((int)corrLen);
        Assert.Equal("rpc://prod/app/echo", reader.ReadString());
        Assert.Equal(string.Empty, reader.ReadString());
        Assert.Equal((uint)4, reader.ReadU32());
        Assert.Equal("ping", System.Text.Encoding.UTF8.GetString(reader.ReadBytes(4)));
    }
}