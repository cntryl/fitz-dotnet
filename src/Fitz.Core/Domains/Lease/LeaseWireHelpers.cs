using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

static class LeaseWireHelpers
{
    static readonly WireFailureFactory<LeaseException> Failure = static failure =>
        new LeaseException(failure.Message, failure.Code, failure.Status, failure.DomainCode)
        {
            ErrorClass = failure.ErrorClass,
            ErrorEffect = failure.ErrorEffect,
        };

    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation) =>
        ResponseEnvelope.ReadSuccess(response, operation, Failure);
}
