using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameCodecEdgeCaseTests
{
    [Fact]
    public void should_round_trip_message_type_zero()
    {
        var payload = new byte[] { 0xFF, 0x00, 0x01 };
        var encoded = FrameCodec.Encode(0, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal((ushort)0, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload);
    }

    [Fact]
    public void should_encode_type_254_as_single_byte()
    {
        var payload = new byte[] { 0x11, 0x22 };
        var encoded = FrameCodec.Encode(254, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal((ushort)254, decoded.MessageType);
        Assert.NotEqual((byte)0xFF, encoded[0]);
    }

    [Fact]
    public void should_require_escape_for_type_255()
    {
        var payload = new byte[] { 0x33, 0x44 };
        var encoded = FrameCodec.Encode(255, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal((ushort)255, decoded.MessageType);
        Assert.Equal((byte)0xFF, encoded[0]);
    }

    [Fact]
    public void should_round_trip_type_256()
    {
        var payload = new byte[] { 0x55, 0x66 };
        var encoded = FrameCodec.Encode(256, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal((ushort)256, decoded.MessageType);
    }

    [Fact]
    public void should_round_trip_maximum_message_type()
    {
        var payload = new byte[] { 0x77, 0x88 };
        var encoded = FrameCodec.Encode(65535, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal((ushort)65535, decoded.MessageType);
    }

    [Fact]
    public void should_round_trip_empty_payload()
    {
        var payload = new byte[0];
        var encoded = FrameCodec.Encode(100, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Empty(decoded.Payload);
    }

    [Fact]
    public void should_round_trip_large_payload()
    {
        var payload = new byte[4096];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(999, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal(payload, decoded.Payload);
    }

    [Fact]
    public void should_round_trip_maximum_size_payload()
    {
        var payload = new byte[65535];
        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(500, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal((ushort)500, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(127)]
    [InlineData(254)]
    public void should_use_single_byte_for_small_types(ushort messageType)
    {
        var payload = new byte[] { 0xFF };
        var encoded = FrameCodec.Encode(messageType, payload);
        Assert.NotEqual((byte)0xFF, encoded[0]);
    }

    [Theory]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(1000)]
    [InlineData(65535)]
    public void should_use_escape_for_large_types(ushort messageType)
    {
        var payload = new byte[] { 0xAA };
        var encoded = FrameCodec.Encode(messageType, payload);
        Assert.Equal((byte)0xFF, encoded[0]);
    }

    [Fact]
    public void should_preserve_all_byte_values()
    {
        var payload = new byte[256];
        for (int i = 0; i < 256; i++)
            payload[i] = (byte)i;
        var encoded = FrameCodec.Encode(777, payload);
        var decoded = FrameCodec.Decode(encoded);
        Assert.Equal(payload, decoded.Payload);
    }
}
