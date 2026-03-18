using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameCodecTests
{
    [Fact]
    public void EncodeDecode_RoundTripsStandardMessageType()
    {
        var payload = new byte[] { 1, 2, 3, 4 };

        var encoded = FrameCodec.Encode(100, payload);
        var decoded = FrameCodec.Decode(encoded);

        Assert.Equal((ushort)100, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload);
    }

    [Fact]
    public void EncodeDecode_RoundTripsExtendedMessageType()
    {
        var payload = "hello"u8.ToArray();

        var encoded = FrameCodec.Encode(700, payload);
        var decoded = FrameCodec.Decode(encoded);

        Assert.Equal((ushort)700, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload);
    }
}