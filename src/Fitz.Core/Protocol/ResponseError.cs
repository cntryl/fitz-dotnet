namespace Cntryl.Fitz.Protocol;

readonly record struct ResponseErrorEnvelope(ushort Code, byte Class, byte Effect, string Message);

static class ResponseError
{
    internal static ResponseErrorEnvelope Read(ReadOnlyMemory<byte> response)
    {
        var reader = new BinaryBufferReader(response);
        if (reader.ReadU8() != 1)
        {
            throw new ProtocolException("Error response must begin with status 1.");
        }

        var code = reader.ReadU16();
        var errorClass = reader.ReadU8();
        var effect = reader.ReadU8();
        if (effect > 2)
        {
            throw new ProtocolException("Error response contains an unknown effect.");
        }
        if (errorClass > 10)
        {
            errorClass = 6;
        }
        var message = reader.ReadString();
        if (!reader.IsEof)
        {
            throw new ProtocolException("Error response has trailing bytes.");
        }
        return new ResponseErrorEnvelope(code, errorClass, effect, message);
    }
}
