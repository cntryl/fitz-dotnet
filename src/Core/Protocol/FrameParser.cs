using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

public sealed class FrameParser
{
    private const int InitialCapacity = 1024;
    private const int DefaultMaxBufferSize = ushort.MaxValue + FrameCodec.MaxHeaderSize;

    private readonly int _maxBufferSize;
    private byte[] _buffer = new byte[InitialCapacity];
    private int _length;
    private int _readOffset;

    public FrameParser()
        : this(DefaultMaxBufferSize)
    {
    }

    public FrameParser(int maxBufferSize)
    {
        if (maxBufferSize < ushort.MaxValue + FrameCodec.MaxHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBufferSize), "Max buffer size must accommodate at least one full frame.");
        }

        _maxBufferSize = maxBufferSize;
    }

    public void Append(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
        {
            return;
        }

        EnsureCapacity(_length + data.Length);
        data.CopyTo(_buffer.AsSpan(_length));
        _length += data.Length;
    }

    public bool TryReadFrame(out Frame frame)
    {
        frame = default;

        if (_readOffset >= _length)
        {
            _readOffset = 0;
            _length = 0;
            return false;
        }

        var source = _buffer.AsSpan(_readOffset, _length - _readOffset);
        var offset = 0;

        if (source.Length < 3)
        {
            return false;
        }

        ushort messageType;
        var first = source[offset++];
        if (first == 0xFF)
        {
            if (source.Length < 5)
            {
                return false;
            }

            messageType = BinaryPrimitives.ReadUInt16BigEndian(source.Slice(offset, 2));
            offset += 2;
        }
        else
        {
            messageType = first;
        }

        if (source.Length - offset < 2)
        {
            return false;
        }

        var payloadLength = BinaryPrimitives.ReadUInt16BigEndian(source.Slice(offset, 2));
        offset += 2;
        if (source.Length - offset < payloadLength)
        {
            return false;
        }

        frame = new Frame(messageType, _buffer.AsMemory(_readOffset + offset, payloadLength));
        _readOffset += offset + payloadLength;

        if (_readOffset == _length)
        {
            _readOffset = 0;
            _length = 0;
        }

        return true;
    }

    public IReadOnlyList<Frame> ParseFrames(ReadOnlySpan<byte> data)
    {
        Append(data);

        List<Frame>? frames = null;
        while (TryReadFrame(out var frame))
        {
            frames ??= new List<Frame>();
            if (frame.Payload.IsEmpty)
            {
                frames.Add(frame);
            }
            else
            {
                frames.Add(new Frame(frame.MessageType, frame.Payload.ToArray()));
            }
        }

        return frames is null ? Array.Empty<Frame>() : frames;
    }

    private void EnsureCapacity(int required)
    {
        if (required <= _buffer.Length)
        {
            return;
        }

        if (required > _maxBufferSize)
        {
            throw new InvalidOperationException($"Frame accumulator exceeded max buffer size {_maxBufferSize}.");
        }

        if (_readOffset > 0)
        {
            var unread = _length - _readOffset;
            if (unread > 0)
            {
                Buffer.BlockCopy(_buffer, _readOffset, _buffer, 0, unread);
            }

            _length = unread;
            _readOffset = 0;
            if (required <= _buffer.Length)
            {
                return;
            }
        }

        var next = _buffer.Length;
        while (next < required)
        {
            next *= 2;
        }

        if (next > _maxBufferSize)
        {
            next = _maxBufferSize;
        }

        Array.Resize(ref _buffer, next);
    }
}