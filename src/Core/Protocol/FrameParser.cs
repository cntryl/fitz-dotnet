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
        if (!FrameCodec.TryReadHeader(source, out var messageType, out var payloadLength, out var headerLength))
        {
            return false;
        }

        if (source.Length - headerLength < payloadLength)
        {
            return false;
        }

        frame = new Frame(messageType, _buffer.AsMemory(_readOffset + headerLength, payloadLength));
        _readOffset += headerLength + payloadLength;

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