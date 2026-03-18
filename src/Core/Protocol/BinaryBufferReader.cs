using System.Buffers.Binary;
using System.Text;

namespace Cntryl.Fitz.Protocol;

public sealed class BinaryBufferReader
{
    private readonly byte[] _data;
    private int _offset;

    public BinaryBufferReader(byte[] data)
    {
        _data = data ?? throw new ArgumentNullException(nameof(data));
    }

    public bool IsEof => _offset >= _data.Length;

    public int RemainingBytes => _data.Length - _offset;

    public byte ReadU8()
    {
        EnsureAvailable(1);
        return _data[_offset++];
    }

    public uint ReadU32()
    {
        EnsureAvailable(4);
        var value = BinaryPrimitives.ReadUInt32BigEndian(_data.AsSpan(_offset, 4));
        _offset += 4;
        return value;
    }

    public ulong ReadU64()
    {
        EnsureAvailable(8);
        var value = BinaryPrimitives.ReadUInt64BigEndian(_data.AsSpan(_offset, 8));
        _offset += 8;
        return value;
    }

    public byte[] ReadBytes(int length)
    {
        EnsureAvailable(length);
        var result = _data.AsSpan(_offset, length).ToArray();
        _offset += length;
        return result;
    }

    public string ReadString()
    {
        var length = checked((int)ReadU32());
        return Encoding.UTF8.GetString(ReadBytes(length));
    }

    private void EnsureAvailable(int count)
    {
        if (RemainingBytes < count)
        {
            throw new InvalidOperationException($"Buffer overflow: cannot read {count} bytes.");
        }
    }
}