using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class FrameParserEdgeCaseTests
{
    [Fact]
    public void should_parse_message_type_zero()
    {
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(0, [0xAA, 0xBB]);
        var frames = parser.ParseFrames(encoded);
        Assert.Single(frames);
        Assert.Equal((ushort)0, frames[0].MessageType);
    }

    [Fact]
    public void should_parse_message_type_254()
    {
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(254, [0x11, 0x22]);
        var frames = parser.ParseFrames(encoded);
        Assert.Single(frames);
        Assert.Equal((ushort)254, frames[0].MessageType);
    }

    [Fact]
    public void should_parse_message_type_255_escape_boundary()
    {
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(255, [0x33, 0x44]);
        var frames = parser.ParseFrames(encoded);
        Assert.Single(frames);
        Assert.Equal((ushort)255, frames[0].MessageType);
    }

    [Fact]
    public void should_parse_large_message_type()
    {
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(65535, [0x55, 0x66]);
        var frames = parser.ParseFrames(encoded);
        Assert.Single(frames);
        Assert.Equal((ushort)65535, frames[0].MessageType);
    }

    [Fact]
    public void should_parse_empty_payload()
    {
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(500, []);
        var frames = parser.ParseFrames(encoded);
        Assert.Single(frames);
        Assert.True(frames[0].Payload.IsEmpty);
    }

    [Fact]
    public void should_handle_byte_by_byte_fragmentation()
    {
        var parser = new FrameParser();
        var encoded = FrameCodec.Encode(100, [0xAA, 0xBB, 0xCC]);
        var parseResults = new List<IReadOnlyList<Frame>>();

        for (int i = 0; i < encoded.Length; i++)
        {
            var chunk = encoded.AsSpan(i, 1).ToArray();
            parseResults.Add(parser.ParseFrames(chunk));
        }

        for (int i = 0; i < encoded.Length - 1; i++)
            Assert.Empty(parseResults[i]);
        Assert.Single(parseResults[^1]);
        Assert.Equal((ushort)100, parseResults[^1][0].MessageType);
    }

    [Fact]
    public void should_parse_multiple_mixed_type_frames()
    {
        var parser = new FrameParser();
        var first = FrameCodec.Encode(100, [0x11]);
        var second = FrameCodec.Encode(300, [0x22]);
        var third = FrameCodec.Encode(0, [0x33]);
        var input = new byte[first.Length + second.Length + third.Length];
        first.CopyTo(input, 0);
        second.CopyTo(input, first.Length);
        third.CopyTo(input, first.Length + second.Length);

        var frames = parser.ParseFrames(input);
        Assert.Equal(3, frames.Count);
        Assert.Equal((ushort)100, frames[0].MessageType);
        Assert.Equal((ushort)300, frames[1].MessageType);
        Assert.Equal((ushort)0, frames[2].MessageType);
    }

    [Fact]
    public void should_handle_fragmentation_at_type_boundary()
    {
        var parser = new FrameParser();
        var frame254 = FrameCodec.Encode(254, [0xAA]);
        var frame255 = FrameCodec.Encode(255, [0xBB]);
        var combined = new byte[frame254.Length + frame255.Length];
        frame254.CopyTo(combined, 0);
        frame255.CopyTo(combined, frame254.Length);

        var midpoint = frame254.Length + 1;
        var first = parser.ParseFrames(combined.AsSpan(0, midpoint).ToArray());
        var second = parser.ParseFrames(combined.AsSpan(midpoint).ToArray());

        Assert.Single(first);
        Assert.Equal((ushort)254, first[0].MessageType);
        Assert.Single(second);
        Assert.Equal((ushort)255, second[0].MessageType);
    }

    [Fact]
    public void should_parse_large_payload()
    {
        var parser = new FrameParser();
        var largePayload = new byte[4096];
        for (int i = 0; i < largePayload.Length; i++)
            largePayload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(600, largePayload);

        var frames = parser.ParseFrames(encoded);
        Assert.Single(frames);
        Assert.Equal((ushort)600, frames[0].MessageType);
        Assert.Equal(largePayload, frames[0].Payload.ToArray());
    }

    [Fact]
    public void should_handle_large_payload_fragmentation()
    {
        var parser = new FrameParser();
        var largePayload = new byte[2048];
        for (int i = 0; i < largePayload.Length; i++)
            largePayload[i] = (byte)(i % 256);
        var encoded = FrameCodec.Encode(700, largePayload);

        var oneThird = encoded.Length / 3;
        var twoThirds = (encoded.Length * 2) / 3;
        var first = parser.ParseFrames(encoded.AsSpan(0, oneThird).ToArray());
        var second = parser.ParseFrames(encoded.AsSpan(oneThird, twoThirds - oneThird).ToArray());
        var third = parser.ParseFrames(encoded.AsSpan(twoThirds).ToArray());

        Assert.Empty(first);
        Assert.Empty(second);
        Assert.Single(third);
        Assert.Equal((ushort)700, third[0].MessageType);
    }

    [Fact]
    public void should_maintain_state_across_calls()
    {
        var parser = new FrameParser();
        var frame1 = FrameCodec.Encode(100, [0x11, 0x22, 0x33]);
        var frame2 = FrameCodec.Encode(200, [0x44, 0x55, 0x66]);

        var result1 = parser.ParseFrames(frame1.AsSpan(0, frame1.Length / 2).ToArray());
        var result2 = parser.ParseFrames(frame1.AsSpan(frame1.Length / 2).ToArray());
        var result3 = parser.ParseFrames(frame2);

        Assert.Empty(result1);
        Assert.Single(result2);
        Assert.Single(result3);
        Assert.Equal((ushort)100, result2[0].MessageType);
        Assert.Equal((ushort)200, result3[0].MessageType);
    }

    [Fact]
    public void should_parse_empty_input()
    {
        var parser = new FrameParser();
        var frames = parser.ParseFrames([]);
        Assert.Empty(frames);
    }
}
