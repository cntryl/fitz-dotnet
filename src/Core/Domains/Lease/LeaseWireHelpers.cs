using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

internal static class LeaseWireHelpers
{
    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation)
    {
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status == 0)
        {
            return reader;
        }

        if (status != 1)
        {
            throw new LeaseException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        var domainCode = reader.ReadU32();
        var message = reader.ReadString();
        if (!reader.IsEof)
        {
            throw new LeaseException($"{operation} error response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }

        throw new LeaseException($"{operation} failed: {message}", $"{operation}_FAILED", status, domainCode);
    }
}
