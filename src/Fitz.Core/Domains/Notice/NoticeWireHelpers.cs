using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Notice;

static class NoticeWireHelpers
{
    static readonly WireFailureFactory<NoticeException> Failure = static failure =>
        new NoticeException(failure.Message, failure.Code, failure.Status, failure.DomainCode)
        {
            ErrorClass = failure.ErrorClass,
            ErrorEffect = failure.ErrorEffect,
        };

    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation) =>
        ResponseEnvelope.ReadSuccess(response, operation, Failure);
}
