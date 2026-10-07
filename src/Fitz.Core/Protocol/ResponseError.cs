using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

static class ResponseError
{
    internal static (uint? Code, string Message) Read(ReadOnlyMemory<byte> response, bool codedOnly = false)
    {
        var payload = response.Span;
        var plain = payload.Length >= 5 && BinaryPrimitives.ReadUInt32BigEndian(payload.Slice(1, 4)) == payload.Length - 5;
        var coded = payload.Length >= 9 && BinaryPrimitives.ReadUInt32BigEndian(payload.Slice(5, 4)) == payload.Length - 9;
        if (payload.IsEmpty || payload[0] != 1 || (codedOnly ? !coded : plain == coded))
        {
            throw new ProtocolException("Error response is malformed or ambiguous");
        }

        var reader = new BinaryBufferReader(response);
        reader.ReadU8();
        uint? code = coded ? reader.ReadU32() : null;
        return (code, reader.ReadString());
    }
}
