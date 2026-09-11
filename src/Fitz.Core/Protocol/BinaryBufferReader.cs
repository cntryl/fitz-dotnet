using System.Buffers.Binary;
using System.Text;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Protocol;

/// <summary>
/// Sequential reader for Fitz wire payloads.
/// </summary>
/// <remarks>
/// Every read validates that enough bytes remain and throws rather than returning partial
/// data, so a malformed payload fails closed. Not thread-safe.
/// </remarks>
public sealed class BinaryBufferReader
{
    readonly ReadOnlyMemory<byte> _data;
    int _offset;

    /// <summary>Creates a reader over an array.</summary>
    /// <param name="data">Bytes to read.</param>
    public BinaryBufferReader(byte[] data)
        : this(data.AsMemory())
    {
    }

    /// <summary>Creates a reader over a memory region.</summary>
    /// <param name="data">Bytes to read.</param>
    public BinaryBufferReader(ReadOnlyMemory<byte> data)
    {
        _data = data;
    }

    /// <summary>Whether every byte has been consumed.</summary>
    public bool IsEof => _offset >= _data.Length;

    /// <summary>Bytes not yet consumed.</summary>
    public int RemainingBytes => _data.Length - _offset;
    internal int Offset => _offset;
    internal void SetOffset(int offset) => _offset = offset;

    /// <summary>Reads one byte.</summary>
    /// <returns>The byte read.</returns>
    public byte ReadU8()
    {
        EnsureAvailable(1);
        return _data.Span[_offset++];
    }

    /// <summary>Reads a big-endian 32-bit unsigned integer.</summary>
    /// <returns>The value read.</returns>
    public uint ReadU32()
    {
        EnsureAvailable(4);
        var value = BinaryPrimitives.ReadUInt32BigEndian(_data.Span.Slice(_offset, 4));
        _offset += 4;
        return value;
    }

    /// <summary>Reads a big-endian 64-bit unsigned integer.</summary>
    /// <returns>The value read.</returns>
    public ulong ReadU64()
    {
        EnsureAvailable(8);
        var value = BinaryPrimitives.ReadUInt64BigEndian(_data.Span.Slice(_offset, 8));
        _offset += 8;
        return value;
    }

    /// <summary>Reads bytes into a new array.</summary>
    /// <param name="length">Number of bytes to read.</param>
    /// <returns>A copy of the bytes read.</returns>
    public byte[] ReadBytes(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = GC.AllocateUninitializedArray<byte>(length);
        _data.Span.Slice(_offset, length).CopyTo(result);
        _offset += length;
        return result;
    }

    /// <summary>Reads bytes into a new array, validating the wire length first.</summary>
    /// <param name="length">Number of bytes to read, as read from the wire.</param>
    /// <returns>A copy of the bytes read.</returns>
    public byte[] ReadBytes(uint length) => ReadBytes(ReadLength(length));

    /// <summary>Reads bytes without copying.</summary>
    /// <param name="length">Number of bytes to read.</param>
    /// <returns>A view over the underlying buffer.</returns>
    public ReadOnlyMemory<byte> ReadMemory(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = _data.Slice(_offset, length);
        _offset += length;
        return result;
    }

    /// <summary>Reads bytes without copying, validating the wire length first.</summary>
    /// <param name="length">Number of bytes to read, as read from the wire.</param>
    /// <returns>A view over the underlying buffer.</returns>
    public ReadOnlyMemory<byte> ReadMemory(uint length) => ReadMemory(ReadLength(length));

    /// <summary>Reads bytes as a span without copying.</summary>
    /// <param name="length">Number of bytes to read.</param>
    /// <returns>A span over the underlying buffer.</returns>
    public ReadOnlySpan<byte> ReadSpan(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = _data.Span.Slice(_offset, length);
        _offset += length;
        return result;
    }

    /// <summary>Reads bytes as a span, validating the wire length first.</summary>
    /// <param name="length">Number of bytes to read, as read from the wire.</param>
    /// <returns>A span over the underlying buffer.</returns>
    public ReadOnlySpan<byte> ReadSpan(uint length) => ReadSpan(ReadLength(length));

    /// <summary>Reads a length-prefixed UTF-8 string.</summary>
    /// <returns>The decoded string.</returns>
    public string ReadString()
    {
        var length = ReadLength(ReadU32());
        return Encoding.UTF8.GetString(ReadSpan(length));
    }

    /// <summary>Reads a length prefix and validates it against the remaining bytes.</summary>
    /// <returns>The validated length.</returns>
    public int ReadLength() => ReadLength(ReadU32());

    void EnsureAvailable(int count)
    {
        if (RemainingBytes < count)
        {
            throw new ProtocolException($"Protocol payload is truncated: requested {count} bytes with only {RemainingBytes} remaining.");
        }
    }

    int ReadLength(uint length)
    {
        if (length > int.MaxValue || length > RemainingBytes)
        {
            throw new ProtocolException($"Protocol length {length} exceeds the {RemainingBytes} bytes remaining in the payload.");
        }

        return (int)length;
    }
}
