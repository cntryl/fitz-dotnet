namespace Cntryl.Fitz.Protocol;

public sealed class FrameParser
{
    private readonly List<byte> _buffer = [];

    public IReadOnlyList<Frame> ParseFrames(ReadOnlySpan<byte> data)
    {
        if (!data.IsEmpty)
        {
            _buffer.AddRange(data.ToArray());
        }

        var frames = new List<Frame>();
        var offset = 0;

        while (TryReadFrame(_buffer, ref offset, out var frame))
        {
            frames.Add(frame);
        }

        if (offset > 0)
        {
            _buffer.RemoveRange(0, offset);
        }

        return frames;
    }

    private static bool TryReadFrame(IReadOnlyList<byte> source, ref int offset, out Frame frame)
    {
        frame = default;
        if (offset >= source.Count)
        {
            return false;
        }

        var start = offset;
        if (source.Count - offset < 3)
        {
            return false;
        }

        ushort messageType;
        var first = source[offset++];
        if (first == 0xFF)
        {
            if (source.Count - offset < 2)
            {
                offset = start;
                return false;
            }

            messageType = (ushort)((source[offset++] << 8) | source[offset++]);
        }
        else
        {
            messageType = first;
        }

        if (source.Count - offset < 2)
        {
            offset = start;
            return false;
        }

        var payloadLength = (source[offset++] << 8) | source[offset++];
        if (source.Count - offset < payloadLength)
        {
            offset = start;
            return false;
        }

        var payload = new byte[payloadLength];
        for (var i = 0; i < payloadLength; i++)
        {
            payload[i] = source[offset + i];
        }

        offset += payloadLength;
        frame = new Frame(messageType, payload);
        return true;
    }
}