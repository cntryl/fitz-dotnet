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

    internal static QueueException ReadError(ReadOnlyMemory<byte> response, string operation, byte status, bool codedOnly = false)
    {
        try
        {
            var (code, message) = ResponseError.Read(response, codedOnly);
            var description = operation == "ENQUEUE" ? message : $"{operation} failed: {message}";
            return new QueueException(description, $"{operation}_FAILED", status, code);
        }
        catch (ProtocolException)
        {
            return new QueueException($"{operation} error response is malformed or ambiguous", $"{operation}_INVALID_RESPONSE", status);
        }
    }
}
