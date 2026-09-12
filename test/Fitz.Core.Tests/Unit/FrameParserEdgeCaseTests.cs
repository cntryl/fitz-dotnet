using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameParserEdgeCaseTests
{
    [Fact]
    public void ShouldParseMessageTypeZeroGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(0, [0xAA, 0xBB]);

        // Act
        var frames = parser.ParseFrames(encoded);

        // Assert
        Assert.Single(frames);
        Assert.Equal((ushort)0, frames[0].MessageType);
    }

    [Fact]
    public void ShouldParseMessageType254GivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(254, [0x11, 0x22]);

        // Act
        var frames = parser.ParseFrames(encoded);

        // Assert
        Assert.Single(frames);
        Assert.Equal((ushort)254, frames[0].MessageType);
    }

    [Fact]
    public void ShouldParseMessageType255EscapeBoundaryGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(255, [0x33, 0x44]);

        // Act
        var frames = parser.ParseFrames(encoded);

        // Assert
        Assert.Single(frames);
        Assert.Equal((ushort)255, frames[0].MessageType);
    }

    [Fact]
    public void ShouldParseLargeMessageTypeGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(65535, [0x55, 0x66]);

        // Act
        var frames = parser.ParseFrames(encoded);

        // Assert
        Assert.Single(frames);
        Assert.Equal((ushort)65535, frames[0].MessageType);
    }

    [Fact]
    public void ShouldParseEmptyPayloadGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(500, []);

        // Act
        var frames = parser.ParseFrames(encoded);

        // Assert
        Assert.Single(frames);
        Assert.True(frames[0].Payload.IsEmpty);
    }

    [Fact]
    public void ShouldHandleByteByByteFragmentationGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(100, [0xAA, 0xBB, 0xCC]);
        var parseResults = new List<IReadOnlyList<Frame>>();


        // Act
        for (var i = 0; i < encoded.Length; i++)
        {
            var chunk = encoded.AsSpan(i, 1).ToArray();
            parseResults.Add(parser.ParseFrames(chunk));
        }

        // Assert
        for (var i = 0; i < encoded.Length - 1; i++)
            Assert.Empty(parseResults[i]);
        Assert.Single(parseResults[^1]);
        Assert.Equal((ushort)100, parseResults[^1][0].MessageType);
    }

    [Fact]
    public void ShouldParseMultipleMixedTypeFramesGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var first = FrameCodec.Encode(100, [0x11]);
        var second = FrameCodec.Encode(300, [0x22]);
        var third = FrameCodec.Encode(0, [0x33]);
        var input = new byte[first.Length + second.Length + third.Length];
        first.CopyTo(input, 0);
        second.CopyTo(input, first.Length);
        third.CopyTo(input, first.Length + second.Length);


        // Act
        var frames = parser.ParseFrames(input);

        // Assert
        Assert.Equal(3, frames.Count);
        Assert.Equal((ushort)100, frames[0].MessageType);
        Assert.Equal((ushort)300, frames[1].MessageType);
        Assert.Equal((ushort)0, frames[2].MessageType);
    }

    [Fact]
    public void ShouldHandleFragmentationAtTypeBoundaryGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var frame254 = FrameCodec.Encode(254, [0xAA]);
        var frame255 = FrameCodec.Encode(255, [0xBB]);
        var combined = new byte[frame254.Length + frame255.Length];
        frame254.CopyTo(combined, 0);
        frame255.CopyTo(combined, frame254.Length);

        var midpoint = frame254.Length + 1;
        var first = parser.ParseFrames(combined.AsSpan(0, midpoint).ToArray());

        // Act
        var second = parser.ParseFrames(combined.AsSpan(midpoint).ToArray());


        // Assert
        Assert.Single(first);
        Assert.Equal((ushort)254, first[0].MessageType);
        Assert.Single(second);
        Assert.Equal((ushort)255, second[0].MessageType);
    }

    [Fact]
    public void ShouldParseLargePayloadGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var largePayload = new byte[4096];
        for (var i = 0; i < largePayload.Length; i++)
            largePayload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(600, largePayload);


        // Act
        var frames = parser.ParseFrames(encoded);

        // Assert
        Assert.Single(frames);
        Assert.Equal((ushort)600, frames[0].MessageType);
        Assert.Equal(largePayload, frames[0].Payload.ToArray());
    }

    [Fact]
    public void ShouldHandleLargePayloadFragmentationGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var largePayload = new byte[2048];
        for (var i = 0; i < largePayload.Length; i++)
            largePayload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(700, largePayload);

        var oneThird = encoded.Length / 3;
        var twoThirds = (encoded.Length * 2) / 3;
        var first = parser.ParseFrames(encoded.AsSpan(0, oneThird).ToArray());
        var second = parser.ParseFrames(encoded.AsSpan(oneThird, twoThirds - oneThird).ToArray());

        // Act
        var third = parser.ParseFrames(encoded.AsSpan(twoThirds).ToArray());


        // Assert
        Assert.Empty(first);
        Assert.Empty(second);
        Assert.Single(third);
        Assert.Equal((ushort)700, third[0].MessageType);
    }

    [Fact]
    public void ShouldMaintainStateAcrossCallsGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();
        var frame1 = FrameCodec.Encode(100, [0x11, 0x22, 0x33]);
        var frame2 = FrameCodec.Encode(200, [0x44, 0x55, 0x66]);

        var result1 = parser.ParseFrames(frame1.AsSpan(0, frame1.Length / 2).ToArray());
        var result2 = parser.ParseFrames(frame1.AsSpan(frame1.Length / 2).ToArray());

        // Act
        var result3 = parser.ParseFrames(frame2);


        // Assert
        Assert.Empty(result1);
        Assert.Single(result2);
        Assert.Single(result3);
        Assert.Equal((ushort)100, result2[0].MessageType);
        Assert.Equal((ushort)200, result3[0].MessageType);
    }

    [Fact]
    public void ShouldParseEmptyInputGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser();

        // Act
        var frames = parser.ParseFrames([]);

        // Assert
        Assert.Empty(frames);
    }

    [Fact]
    public void ShouldRejectFragmentedDataBeforeExceedingCustomBufferCapGivenProtocolBytesWhenParsing()
    {
        // Arrange
        var parser = new FrameParser(100);
        var oversizedFrame = FrameCodec.Encode(100, new byte[100]);


        // Act
        var frames = parser.ParseFrames(oversizedFrame.AsSpan(0, 60));


        // Assert
        Assert.Empty(frames);
        var error = Assert.Throws<ProtocolException>(() => parser.Append(oversizedFrame.AsSpan(60, 41)));
        Assert.Contains("100", error.Message, StringComparison.Ordinal);
    }
}
