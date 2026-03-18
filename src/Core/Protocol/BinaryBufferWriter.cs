using System.Buffers.Binary;
using System.Text;

namespace Cntryl.Fitz.Protocol;

public sealed class BinaryBufferWriter
{
    private readonly MemoryStream _stream = new();

    public void WriteU8(byte value)
    {
        _stream.WriteByte(value);
    }

    public void WriteU32(uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        _stream.Write(buffer);
    }

    public void WriteU64(ulong value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        _stream.Write(buffer);
    }

    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        _stream.Write(bytes);
    }

    public void WriteString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteU32((uint)bytes.Length);
        WriteBytes(bytes);
    }

    public byte[] Build() => _stream.ToArray();
}