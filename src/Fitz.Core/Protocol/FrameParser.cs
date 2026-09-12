using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

/// <summary>
/// Buffers framed bytes and exposes complete Fitz protocol frames.
/// </summary>
sealed class FrameParser
{
    const int InitialCapacity = 1024;
    const int DefaultMaxBufferSize = FrameCodec.MaxTransportFrameSize;

    readonly int _maxBufferSize;
    byte[] _buffer;
    int _length;
    int _readOffset;

    /// <summary>
    /// Creates a parser with the default maximum buffer size.
    /// </summary>
    public FrameParser()
        : this(DefaultMaxBufferSize)
    {
    }

    /// <summary>
    /// Creates a parser with a custom maximum buffer size.
    /// </summary>
    public FrameParser(int maxBufferSize)
    {
        if (maxBufferSize < FrameCodec.MaxHeaderSize)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBufferSize), "Max buffer size must accommodate a Fitz frame header.");
        }

        _maxBufferSize = maxBufferSize;
        _buffer = new byte[Math.Min(InitialCapacity, maxBufferSize)];
    }

    /// <summary>
    /// Appends raw frame bytes to the internal buffer.
    /// </summary>
    public void Append(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
        {
            return;
        }

        Compact();

        int required;
        try
        {
            required = checked(_length + data.Length);
        }
        catch (OverflowException exception)
        {
            throw new ProtocolException("Frame accumulator length overflowed.", exception);
        }

        EnsureCapacity(required);
        data.CopyTo(_buffer.AsSpan(_length));
        _length += data.Length;
    }

    /// <summary>
    /// Attempts to read the next complete frame from the buffered data. The returned payload
    /// borrows parser storage and is valid only until the next call to <see cref="Append"/> or
    /// <see cref="TryReadFrame(out Frame)"/>.
    /// </summary>
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

    /// <summary>
    /// Appends data and returns all complete frames that can be parsed. Returned payloads own
    /// independent arrays and remain valid after subsequent parser operations.
    /// </summary>
    public IReadOnlyList<Frame> ParseFrames(ReadOnlySpan<byte> data)
    {
        Append(data);

        List<Frame>? frames = null;
        while (TryReadFrame(out var frame))
        {
            frames ??= [];
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

    internal void Reset()
    {
        _length = 0;
        _readOffset = 0;
    }

    void EnsureCapacity(int required)
    {
        if (required > _maxBufferSize)
        {
            throw new ProtocolException($"Frame accumulator exceeded max buffer size {_maxBufferSize}.");
        }

        if (required <= _buffer.Length)
        {
            return;
        }

        var next = _buffer.Length;
        while (next < required)
        {
            next = next <= _maxBufferSize / 2 ? next * 2 : _maxBufferSize;
        }

        if (next > _maxBufferSize)
        {
            next = _maxBufferSize;
        }

        Array.Resize(ref _buffer, next);
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
