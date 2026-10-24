namespace Cntryl.Fitz.Protocol;

/// <summary>Buffers bytes and exposes complete Wire-2 Fitz frames.</summary>
sealed class FrameParser
{
    readonly int _maxBufferSize;
    byte[] _buffer;
    int _length;
    int _readOffset;

    public FrameParser() : this(FrameCodec.MaxTransportFrameSize) { }

    public FrameParser(int maxBufferSize)
    {
        if (maxBufferSize < FrameCodec.MaxHeaderSize || maxBufferSize > FrameCodec.MaxTransportFrameSize)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBufferSize));
        }
        _maxBufferSize = maxBufferSize;
        _buffer = new byte[Math.Min(1024, maxBufferSize)];
    }

    public void Append(ReadOnlySpan<byte> data)
    {
        Compact();
        var required = checked(_length + data.Length);
        if (required > _maxBufferSize)
        {
            throw new ProtocolException($"Frame accumulator exceeded max buffer size {_maxBufferSize}.");
        }
        if (required > _buffer.Length)
        {
            Array.Resize(ref _buffer, Math.Min(_maxBufferSize, Math.Max(required, _buffer.Length * 2)));
        }
        data.CopyTo(_buffer.AsSpan(_length));
        _length = required;
    }

    public bool TryReadFrame(out Frame frame)
    {
        frame = default;
        var source = _buffer.AsSpan(_readOffset, _length - _readOffset);
        if (!FrameCodec.TryReadHeader(source, out var messageType, out var flags, out var correlationId, out var payloadLength))
        {
            return false;
        }
        var frameLength = checked(FrameCodec.MaxHeaderSize + (int)payloadLength);
        if (source.Length < frameLength)
        {
            return false;
        }
        frame = new Frame(messageType, flags, correlationId,
            _buffer.AsMemory(_readOffset + FrameCodec.MaxHeaderSize, (int)payloadLength));
        _readOffset += frameLength;
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
            frames ??= [];
            frames.Add(new Frame(frame.MessageType, frame.Flags, frame.CorrelationId,
                frame.Payload.IsEmpty ? ReadOnlyMemory<byte>.Empty : frame.Payload.ToArray()));
        }
        return frames is null ? Array.Empty<Frame>() : frames;
    }

    internal void Reset()
    {
        _length = 0;
        _readOffset = 0;
    }

    void Compact()
    {
        if (_readOffset == 0)
        {
            return;
        }
        var unread = _length - _readOffset;
        if (unread > 0)
        {
            Buffer.BlockCopy(_buffer, _readOffset, _buffer, 0, unread);
        }
        _length = unread;
        _readOffset = 0;
    }
}
