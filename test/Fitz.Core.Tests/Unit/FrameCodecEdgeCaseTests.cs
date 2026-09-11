using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameCodecEdgeCaseTests
{
    [Fact]
    public void ShouldRoundTripMessageTypeZeroGivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[] { 0xFF, 0x00, 0x01 };
        var encoded = FrameCodec.Encode(0, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)0, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Fact]
    public void ShouldEncodeType254AsSingleByteGivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[] { 0x11, 0x22 };
        var encoded = FrameCodec.Encode(254, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)254, decoded.MessageType);
        Assert.NotEqual((byte)0xFF, encoded[0]);
    }

    [Fact]
    public void ShouldRequireEscapeForType255GivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[] { 0x33, 0x44 };
        var encoded = FrameCodec.Encode(255, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)255, decoded.MessageType);
        Assert.Equal((byte)0xFF, encoded[0]);
    }

    [Fact]
    public void ShouldRoundTripType256GivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[] { 0x55, 0x66 };
        var encoded = FrameCodec.Encode(256, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)256, decoded.MessageType);
    }

    [Fact]
    public void ShouldRoundTripMaximumMessageTypeGivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[] { 0x77, 0x88 };
        var encoded = FrameCodec.Encode(65535, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)65535, decoded.MessageType);
    }

    [Fact]
    public void ShouldRoundTripEmptyPayloadGivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = Array.Empty<byte>();
        var encoded = FrameCodec.Encode(100, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.True(decoded.Payload.IsEmpty);
    }

    [Fact]
    public void ShouldRoundTripLargePayloadGivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[4096];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(999, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Fact]
    public void ShouldRoundTripMaximumSizePayloadGivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[65535];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(500, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal((ushort)500, decoded.MessageType);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(127)]
    [InlineData(254)]
    public void ShouldUseSingleByteForSmallTypesGivenProtocolFrameWhenEncodingOrDecoding(ushort messageType)
    {
        // Arrange
        var payload = new byte[] { 0xFF };

        // Act
        var encoded = FrameCodec.Encode(messageType, payload);

        // Assert
        Assert.NotEqual((byte)0xFF, encoded[0]);
    }

    [Theory]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(1000)]
    [InlineData(65535)]
    public void ShouldUseEscapeForLargeTypesGivenProtocolFrameWhenEncodingOrDecoding(ushort messageType)
    {
        // Arrange
        var payload = new byte[] { 0xAA };

        // Act
        var encoded = FrameCodec.Encode(messageType, payload);

        // Assert
        Assert.Equal((byte)0xFF, encoded[0]);
    }

    [Fact]
    public void ShouldPreserveAllByteValuesGivenProtocolFrameWhenEncodingOrDecoding()
    {
        // Arrange
        var payload = new byte[256];
        for (var i = 0; i < 256; i++)
            payload[i] = (byte)i;
        var encoded = FrameCodec.Encode(777, payload);

        // Act
        var decoded = FrameCodec.DecodeStrict(encoded);

        // Assert
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Fact]
    public void ShouldRejectTrailingBytesGivenExtraFrameDataWhenDecodingStrictly()
    {
        // Arrange
        var encoded = FrameCodec.Encode(100, [0x1, 0x2]);
        var withTrailingBytes = new byte[encoded.Length + 1];
        encoded.CopyTo(withTrailingBytes, 0);

        // Act
        withTrailingBytes[^1] = 0xFF;


        // Assert
        var ex = Assert.Throws<ProtocolException>(() => FrameCodec.DecodeStrict(withTrailingBytes));

        Assert.Equal("Frame has trailing bytes.", ex.Message);
    }

    [Fact]
    public void ShouldRejectTruncatedFrameGivenIncompleteExtendedHeaderWhenDecoding()
    {
        // Arrange
        // Act
        // Assert
        var ex = Assert.Throws<ProtocolException>(() => FrameCodec.DecodeStrict(new byte[] { 0xFF, 0x01, 0x02, 0x03 }));

        Assert.Equal("Extended frame header is incomplete.", ex.Message);
    }

    [Fact]
    public void ShouldRejectOversizedFrameGivenPayloadAboveLimitWhenEncoding()
    {
        // Arrange
        // Act
        // Assert
        var payload = new byte[ushort.MaxValue + 1];

        var ex = Assert.Throws<ProtocolException>(() => FrameCodec.Encode(100, payload));

        Assert.Contains("65535-byte Fitz wire limit", ex.Message, StringComparison.Ordinal);
    }
}
