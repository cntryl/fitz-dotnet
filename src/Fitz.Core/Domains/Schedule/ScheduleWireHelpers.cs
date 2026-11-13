using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Schedule;

static class ScheduleWireHelpers
{
    static readonly WireFailureFactory<ScheduleException> Failure = static failure =>
        new ScheduleException(failure.Message, failure.Code, failure.Status, failure.DomainCode)
        {
            ErrorClass = failure.ErrorClass,
            ErrorEffect = failure.ErrorEffect,
        };

    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation) =>
        ResponseEnvelope.ReadSuccess(response, operation, Failure);
}
