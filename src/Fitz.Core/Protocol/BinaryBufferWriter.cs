using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Cntryl.Fitz.Protocol;

/// <summary>
/// Sequential writer for Fitz wire payloads, backed by a pooled buffer that grows as needed.
/// </summary>
/// <remarks>
/// Dispose returns the buffer to the pool. <see cref="WrittenSpan"/> and
/// <see cref="WrittenMemory"/> are invalid afterwards; use <see cref="Build"/> for a copy
/// that outlives the writer. Not thread-safe.
/// </remarks>
public sealed class BinaryBufferWriter : IDisposable
{
    const int InitialCapacity = 128;

    byte[]? _buffer;
    int _position;
    bool _disposed;

    /// <summary>Creates a writer over a pooled buffer.</summary>
    public BinaryBufferWriter()
    {
        _buffer = ArrayPool<byte>.Shared.Rent(InitialCapacity);
        _position = 0;
    }

    /// <summary>Writes one byte.</summary>
    /// <param name="value">Value to write.</param>
    public void WriteU8(byte value)
    {
        EnsureCapacity(1);
        _buffer![_position++] = value;
    }

    /// <summary>Writes a big-endian 32-bit unsigned integer.</summary>
    /// <param name="value">Value to write.</param>
    public void WriteU32(uint value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer!.AsSpan(_position, 4), value);
        _position += 4;
    }

    /// <summary>Writes a big-endian 64-bit unsigned integer.</summary>
    /// <param name="value">Value to write.</param>
    public void WriteU64(ulong value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteUInt64BigEndian(_buffer!.AsSpan(_position, 8), value);
        _position += 8;
    }

    /// <summary>Writes raw bytes with no length prefix.</summary>
    /// <param name="bytes">Bytes to write.</param>
    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        EnsureCapacity(bytes.Length);
        bytes.CopyTo(_buffer!.AsSpan(_position));
        _position += bytes.Length;
    }

    /// <summary>Writes a length-prefixed UTF-8 string.</summary>
    /// <param name="value">String to write.</param>
    public void WriteString(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        EnsureCapacity(4 + byteCount);
        BinaryPrimitives.WriteUInt32BigEndian(_buffer!.AsSpan(_position, 4), (uint)byteCount);
        _position += 4;
        Encoding.UTF8.GetBytes(value, _buffer.AsSpan(_position, byteCount));
        _position += byteCount;
    }

    /// <summary>Number of bytes written so far.</summary>
    public int WrittenCount
    {
        get
        {
            ThrowIfDisposed();
            return _position;
        }
    }

    /// <summary>The bytes written so far. Invalid after disposal.</summary>
    public ReadOnlySpan<byte> WrittenSpan
    {
        get
        {
            ThrowIfDisposed();
            return _buffer.AsSpan(0, _position);
        }
    }

    /// <summary>The bytes written so far. Invalid after disposal.</summary>
    public ReadOnlyMemory<byte> WrittenMemory
    {
        get
        {
            ThrowIfDisposed();
            return _buffer.AsMemory(0, _position);
        }
    }

    /// <summary>Copies the written bytes into a new array that outlives the writer.</summary>
    /// <returns>A copy of the payload.</returns>
    public byte[] Build()
    {
        ThrowIfDisposed();
        var result = GC.AllocateUninitializedArray<byte>(_position);
        _buffer.AsSpan(0, _position).CopyTo(result);
        return result;
    }

    /// <summary>Returns the pooled buffer. Safe to call more than once.</summary>
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
            ReturnCleared(buffer, _position);
        }
    }

    /// <summary>
    /// Returns a rented buffer after zeroing only the bytes that were written. Payloads can carry
    /// caller data, so the written region is always cleared; the untouched tail never was.
    /// </summary>
    static void ReturnCleared(byte[] buffer, int writtenLength)
    {
        buffer.AsSpan(0, Math.Min(writtenLength, buffer.Length)).Clear();
        ArrayPool<byte>.Shared.Return(buffer);
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
        ReturnCleared(buffer, _position);
        _buffer = newBuffer;
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
