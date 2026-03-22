using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Cntryl.Fitz.Protocol;

public sealed class BinaryBufferWriter : IDisposable
{
    private const int InitialCapacity = 128;

    private byte[] _buffer;
    private int _position;
    private bool _disposed;

    public BinaryBufferWriter()
    {
        _buffer = ArrayPool<byte>.Shared.Rent(InitialCapacity);
        _position = 0;
    }

    public void WriteU8(byte value)
    {
        EnsureCapacity(1);
        _buffer[_position++] = value;
    }

    public void WriteU32(uint value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer.AsSpan(_position, 4), value);
        _position += 4;
    }

    public void WriteU64(ulong value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteUInt64BigEndian(_buffer.AsSpan(_position, 8), value);
        _position += 8;
    }

    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        EnsureCapacity(bytes.Length);
        bytes.CopyTo(_buffer.AsSpan(_position));
        _position += bytes.Length;
    }

    public void WriteString(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        WriteU32((uint)byteCount);
        EnsureCapacity(byteCount);
        Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position, byteCount));
        _position += byteCount;
    }

    public byte[] Build()
    {
        var result = new byte[_position];
        _buffer.AsSpan(0, _position).CopyTo(result);
        return result;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _disposed = true;
        }
    }

    private void EnsureCapacity(int needed)
    {
        if (_position + needed <= _buffer.Length)
        {
            return;
        }

        var newSize = Math.Max(_buffer.Length * 2, _position + needed);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.AsSpan(0, _position).CopyTo(newBuffer);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }
}