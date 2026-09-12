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
}
