using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Cntryl.Fitz.Protocol;

public sealed class BinaryBufferWriter : IDisposable
{
    const int InitialCapacity = 128;

    byte[]? _buffer;
    int _position;
    bool _disposed;

    public BinaryBufferWriter()
    {
        _buffer = ArrayPool<byte>.Shared.Rent(InitialCapacity);
        _position = 0;
    }

    public void WriteU8(byte value)
    {
        EnsureCapacity(1);
        _buffer![_position++] = value;
    }

    public void WriteU32(uint value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer!.AsSpan(_position, 4), value);
        _position += 4;
    }

    public void WriteU64(ulong value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteUInt64BigEndian(_buffer!.AsSpan(_position, 8), value);
        _position += 8;
    }

    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        EnsureCapacity(bytes.Length);
        bytes.CopyTo(_buffer!.AsSpan(_position));
        _position += bytes.Length;
    }

    public void WriteString(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        EnsureCapacity(4 + byteCount);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer!.AsSpan(_position, 4), (uint)byteCount);
        _position += 4;
        Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position, byteCount));
        _position += byteCount;
    }

    public int WrittenCount
    {
        get
        {
            ThrowIfDisposed();
            return _position;
        }
    }

    public ReadOnlySpan<byte> WrittenSpan
    {
        get
        {
            ThrowIfDisposed();
            return _buffer.AsSpan(0, _position);
        }
    }

    public ReadOnlyMemory<byte> WrittenMemory
    {
        get
        {
            ThrowIfDisposed();
            return _buffer.AsMemory(0, _position);
        }
    }

    public byte[] Build()
    {
        ThrowIfDisposed();
        var result = GC.AllocateUninitializedArray<byte>(_position);
        _buffer.AsSpan(0, _position).CopyTo(result);
        return result;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    void EnsureCapacity(int needed)
    {
        ThrowIfDisposed();
        var buffer = _buffer!;
        var required = checked(_position + needed);
        if (required <= buffer.Length)
        {
            return;
        }

        var doubled = buffer.Length <= int.MaxValue / 2 ? buffer.Length * 2 : int.MaxValue;
        var newSize = Math.Max(doubled, required);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        buffer.AsSpan(0, _position).CopyTo(newBuffer);
        ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        _buffer = newBuffer;
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
