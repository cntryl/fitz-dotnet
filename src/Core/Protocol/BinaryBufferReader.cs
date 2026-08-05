using System.Buffers.Binary;
using System.Text;

namespace Cntryl.Fitz.Protocol;

public sealed class BinaryBufferReader
{
    private readonly ReadOnlyMemory<byte> _data;
    private int _offset;

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

        var result = GC.AllocateUninitializedArray<byte>(length);
        ReadSpan(length).CopyTo(result);
        return result;
    }

    public ReadOnlyMemory<byte> ReadMemory(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = _data.Slice(_offset, length);
        _offset += length;
        return result;
    }

    public ReadOnlySpan<byte> ReadSpan(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        EnsureAvailable(length);
        var result = _data.Span.Slice(_offset, length);
        _offset += length;
        return result;
    }

    public string ReadString()
    {
        var length = checked((int)ReadU32());
        return Encoding.UTF8.GetString(ReadSpan(length));
    }

    private void EnsureAvailable(int count)
    {
        if (RemainingBytes < count)
        {
            throw new InvalidOperationException($"Buffer overflow: cannot read {count} bytes.");
        }
    }
}
