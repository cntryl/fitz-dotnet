using System.Text;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

internal static class KvWireHelpers
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

        if (reader.RemainingBytes < 8)
        {
            throw InvalidResponse(operation, "error envelope is truncated");
        }

        var domainCode = reader.ReadU32();
        var messageLength = reader.ReadU32();
        if (messageLength > int.MaxValue || messageLength != reader.RemainingBytes)
        {
            throw InvalidResponse(operation, "error message length is invalid");
        }

        var message = Encoding.UTF8.GetString(reader.ReadSpan((int)messageLength));
        throw new KvException($"{operation} failed: {message}", $"{operation}_FAILED", status, domainCode);
    }

    internal static KvException InvalidResponse(string operation, string reason) =>
        new($"{operation} {reason}", $"{operation}_INVALID_RESPONSE");
}
