using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Queue;

static class QueueWireHelpers
{
    internal static BinaryBufferReader ReadResponse(ReadOnlyMemory<byte> response, string operation)
    {
        if (response.IsEmpty)
        {
            throw new QueueException($"{operation} response is empty", $"{operation}_INVALID_RESPONSE");
        }

        return new BinaryBufferReader(response);
    }

    internal static QueueException ReadError(ReadOnlyMemory<byte> response, string operation, byte status)
    {
        try
        {
            var error = ResponseError.Read(response);
            var description = operation == "ENQUEUE" ? error.Message : $"{operation} failed: {error.Message}";
            var domainError = new QueueException(description, $"{operation}_FAILED", status, error.Code);
            domainError.SetWireError(error.Class, error.Effect);
            return domainError;
        }
        catch (ProtocolException)
        {
            return new QueueException($"{operation} error response is malformed or ambiguous", $"{operation}_INVALID_RESPONSE", status);
        }
    }
}
