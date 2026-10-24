using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

static class LeaseWireHelpers
{
    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation)
    {
        try
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

            var error = ResponseError.Read(response);
            var domainError = new LeaseException($"{operation} failed: {error.Message}", $"{operation}_FAILED", status, error.Code);
            domainError.SetWireError(error.Class, error.Effect);
            throw domainError;
        }
        catch (ProtocolException exception)
        {
            throw new LeaseException($"{operation} response is malformed", $"{operation}_INVALID_RESPONSE", exception);
        }
    }
}
