namespace Cntryl.Fitz.Protocol;

public sealed class FrameParser
{
    private byte[] _buffer = new byte[1024];
    private int _length;

    public IReadOnlyList<Frame> ParseFrames(ReadOnlySpan<byte> data)
    {
        if (!data.IsEmpty)
        {
            EnsureCapacity(_length + data.Length);
            data.CopyTo(_buffer.AsSpan(_length));
            _length += data.Length;
        }

        var frames = new List<Frame>();
        var offset = 0;

        while (TryReadFrame(_buffer.AsSpan(0, _length), ref offset, out var frame))
        {
            frames.Add(frame);
        }

        if (offset > 0)
        {
            var remaining = _length - offset;
            if (remaining > 0)
            {
                _buffer.AsSpan(offset, remaining).CopyTo(_buffer);
            }

            _length = remaining;
        }

        return frames;
    }

    private static bool TryReadFrame(ReadOnlySpan<byte> source, ref int offset, out Frame frame)
    {
        frame = default;
        if (offset >= source.Length)
        {
            return false;
        }

        var start = offset;
        if (source.Length - offset < 3)
        {
            return false;
        }

        ushort messageType;
        var first = source[offset++];
        if (first == 0xFF)
        {
            if (source.Length - offset < 2)
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

        if (source.Length - offset < 2)
        {
            offset = start;
            return false;
        }

        var payloadLength = (source[offset++] << 8) | source[offset++];
        if (source.Length - offset < payloadLength)
        {
            offset = start;
            return false;
        }

        var payload = source.Slice(offset, payloadLength).ToArray();

        offset += payloadLength;
        frame = new Frame(messageType, payload);
        return true;
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _buffer.Length)
        {
            return;
        }

        var next = _buffer.Length;
        while (next < required)
        {
            next *= 2;
        }

        Array.Resize(ref _buffer, next);
    }
}