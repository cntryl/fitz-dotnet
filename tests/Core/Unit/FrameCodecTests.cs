using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameCodecTests
{
    [Fact]
    public void should_round_trip_payload_given_standard_message_type_when_encoding_and_decoding()
    {
        // Arrange
        var payload = new byte[] { 1, 2, 3, 4 };

        // Act
        var encoded = FrameCodec.Encode(100, payload);
        var decoded = FrameCodec.Decode(encoded);

        // Assert
        Assert.Equal((ushort)100, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload);
    }

    [Fact]
    public void should_round_trip_payload_given_extended_message_type_when_encoding_and_decoding()
    {
        // Arrange
        var payload = "hello"u8.ToArray();

        // Act
        var encoded = FrameCodec.Encode(700, payload);
        var decoded = FrameCodec.Decode(encoded);

        // Assert
        Assert.Equal((ushort)700, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload);
    }
}