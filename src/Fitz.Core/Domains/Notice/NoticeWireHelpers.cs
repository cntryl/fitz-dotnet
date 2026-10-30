using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Notice;

static class NoticeWireHelpers
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
                throw new NoticeException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
            }

            var error = ResponseError.Read(response);
            var domainError = new NoticeException($"{operation} failed: {error.Message}", $"{operation}_FAILED", status, error.Code);
            domainError.SetWireError(error.Class, error.Effect);
            throw domainError;
        }
        catch (ProtocolException)
        {
            throw new NoticeException($"{operation} response is malformed", $"{operation}_INVALID_RESPONSE");
        }
    }
}
