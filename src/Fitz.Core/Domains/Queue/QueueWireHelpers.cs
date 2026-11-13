using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Queue;

static class QueueWireHelpers
{
    static readonly WireFailureFactory<QueueException> Failure = static failure =>
        new QueueException(failure.Message, failure.Code, failure.Status, failure.DomainCode)
        {
            ErrorClass = failure.ErrorClass,
            ErrorEffect = failure.ErrorEffect,
        };

    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation) =>
        ResponseEnvelope.ReadSuccess(response, operation, Failure);
}
