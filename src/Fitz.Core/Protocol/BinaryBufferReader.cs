using System.Buffers.Binary;
using System.Text;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Protocol;

public sealed class BinaryBufferReader
{
    readonly ReadOnlyMemory<byte> _data;
    int _offset;

    public BinaryBufferReader(byte[] data)
        : this(data.AsMemory())
    {
    }

    public BinaryBufferReader(ReadOnlyMemory<byte> data)
    {
        _data = data;
    }

    public bool IsEof => _offset >= _data.Length;

    public int RemainingBytes => _data.Length - _offset;
    internal int Offset => _offset;
    internal void SetOffset(int offset) => _offset = offset;

    public byte ReadU8()
    {
        EnsureAvailable(1);
        return _data.Span[_offset++];
    }

    public uint ReadU32()
    {
        EnsureAvailable(4);
        var value = BinaryPrimitives.ReadUInt32BigEndian(_data.Span.Slice(_offset, 4));
        _offset += 4;
        return value;
    }

    public ulong ReadU64()
    {
        EnsureAvailable(8);
        var value = BinaryPrimitives.ReadUInt64BigEndian(_data.Span.Slice(_offset, 8));
        _offset += 8;
        return value;
    }

    public byte[] ReadBytes(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = GC.AllocateUninitializedArray<byte>(length);
        _data.Span.Slice(_offset, length).CopyTo(result);
        _offset += length;
        return result;
    }

    public byte[] ReadBytes(uint length) => ReadBytes(ReadLength(length));

    public ReadOnlyMemory<byte> ReadMemory(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = _data.Slice(_offset, length);
        _offset += length;
        return result;
    }

    public ReadOnlyMemory<byte> ReadMemory(uint length) => ReadMemory(ReadLength(length));

    public ReadOnlySpan<byte> ReadSpan(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = _data.Span.Slice(_offset, length);
        _offset += length;
        return result;
    }

    public ReadOnlySpan<byte> ReadSpan(uint length) => ReadSpan(ReadLength(length));

    public string ReadString()
    {
        var length = ReadLength(ReadU32());
        return Encoding.UTF8.GetString(ReadSpan(length));
    }

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
