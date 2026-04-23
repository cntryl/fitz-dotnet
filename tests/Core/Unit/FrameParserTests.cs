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
        Assert.Equal([0x1, 0x2], frames[0].Payload.ToArray());
        Assert.Equal((ushort)101, frames[1].MessageType);
        Assert.Equal([0xA], frames[1].Payload.ToArray());
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
        Assert.Equal([0x9, 0x8, 0x7, 0x6], after[0].Payload.ToArray());
    }

    [Fact]
    public void should_read_frames_sequentially_given_buffered_data_when_try_reading()
    {
        // Arrange
        var parser = new FrameParser();
        var first = FrameCodec.Encode(100, [0x1, 0x2]);
        var second = FrameCodec.Encode(101, [0xA]);

        parser.Append(first);
        parser.Append(second);

        // Act
        var readFirst = parser.TryReadFrame(out var firstFrame);
        var readSecond = parser.TryReadFrame(out var secondFrame);
        var readThird = parser.TryReadFrame(out var thirdFrame);

        // Assert
        Assert.True(readFirst);
        Assert.True(readSecond);
        Assert.False(readThird);
        Assert.Equal((ushort)100, firstFrame.MessageType);
        Assert.Equal([0x1, 0x2], firstFrame.Payload.ToArray());
        Assert.Equal((ushort)101, secondFrame.MessageType);
        Assert.Equal([0xA], secondFrame.Payload.ToArray());
        Assert.True(thirdFrame.Payload.IsEmpty);
    }

    [Fact]
    public void should_return_detached_payloads_given_parse_frames_when_parser_buffer_is_reused()
    {
        var parser = new FrameParser();
        var firstFrames = parser.ParseFrames(FrameCodec.Encode(100, [0x1, 0x2, 0x3]));

        Assert.Single(firstFrames);

        var firstPayload = firstFrames[0].Payload;
        var secondFrames = parser.ParseFrames(FrameCodec.Encode(101, [0x9, 0x8, 0x7]));

        Assert.Single(secondFrames);
        Assert.Equal([0x1, 0x2, 0x3], firstPayload.ToArray());
        Assert.Equal([0x9, 0x8, 0x7], secondFrames[0].Payload.ToArray());
    }
}
