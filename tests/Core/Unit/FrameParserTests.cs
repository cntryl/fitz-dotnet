using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameParserTests
{
    [Fact]
    public void should_parse_two_frames_given_buffer_with_multiple_frames_when_parsing_once()
    {
        // Arrange
        var parser = new FrameParser();
        var first = FrameCodec.Encode(100, [0x1, 0x2]);
        var second = FrameCodec.Encode(101, [0xA]);
        var input = new byte[first.Length + second.Length];
        first.CopyTo(input, 0);
        second.CopyTo(input, first.Length);

        // Act
        var frames = parser.ParseFrames(input);

        // Assert
        Assert.Equal(2, frames.Count);
        Assert.Equal((ushort)100, frames[0].MessageType);
        Assert.Equal([0x1, 0x2], frames[0].Payload);
        Assert.Equal((ushort)101, frames[1].MessageType);
        Assert.Equal([0xA], frames[1].Payload);
    }

    [Fact]
    public void should_parse_frame_after_second_chunk_given_partial_frame_when_data_arrives_in_two_reads()
    {
        // Arrange
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(302, [0x9, 0x8, 0x7, 0x6]);
        var firstChunk = encoded.AsSpan(0, 3).ToArray();
        var secondChunk = encoded.AsSpan(3).ToArray();

        // Act
        var before = parser.ParseFrames(firstChunk);
        var after = parser.ParseFrames(secondChunk);

        // Assert
        Assert.Empty(before);
        Assert.Single(after);
        Assert.Equal((ushort)302, after[0].MessageType);
        Assert.Equal([0x9, 0x8, 0x7, 0x6], after[0].Payload);
    }
}
