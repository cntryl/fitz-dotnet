using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

static class KvWireHelpers
{
    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation)
    {
        if (response.IsEmpty)
        {
            throw InvalidResponse(operation, "response is empty");
        }

        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status == 0)
        {
            return reader;
        }

        if (status != 1)
        {
            throw InvalidResponse(operation, $"unknown status {status}");
        }

        ResponseErrorEnvelope error;
        try
        {
            error = ResponseError.Read(response);
        }
        catch (ProtocolException exception)
        {
            throw new KvException($"{operation} error response is malformed: {exception.Message}", $"{operation}_INVALID_RESPONSE");
        }
        var domainError = new KvException($"{operation} failed: {error.Message}", $"{operation}_FAILED", status, error.Code);
        domainError.SetWireError(error.Class, error.Effect);
        throw domainError;
    }

    internal static KvException InvalidResponse(string operation, string reason) =>
        new($"{operation} {reason}", $"{operation}_INVALID_RESPONSE");
}
