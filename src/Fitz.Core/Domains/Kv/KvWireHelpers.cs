using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

static class KvWireHelpers
{
    static readonly WireFailureFactory<KvException> Failure = static failure =>
        new KvException(failure.Message, failure.Code, failure.Status, failure.DomainCode)
        {
            ErrorClass = failure.ErrorClass,
            ErrorEffect = failure.ErrorEffect,
        };

    internal static BinaryBufferReader ReadSuccess(ReadOnlyMemory<byte> response, string operation) =>
        ResponseEnvelope.ReadSuccess(response, operation, Failure);

    internal static KvException InvalidResponse(string operation, string reason) =>
        new($"{operation} {reason}", $"{operation}_INVALID_RESPONSE");
}
