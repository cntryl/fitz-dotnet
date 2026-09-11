using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameCodecTests
{
    [Fact]
    public void ShouldRoundTripPayloadGivenStandardMessageTypeWhenEncodingAndDecoding()
    {
        // Arrange
        var payload = new byte[] { 1, 2, 3, 4 };

        // Act
        var encoded = FrameCodec.Encode(100, payload);
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)100, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Fact]
    public void ShouldRoundTripPayloadGivenExtendedMessageTypeWhenEncodingAndDecoding()
    {
        // Arrange
        var payload = "hello"u8.ToArray();

        // Act
        var encoded = FrameCodec.Encode(700, payload);
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)700, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }
}
