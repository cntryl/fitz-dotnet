using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Rpc;

/// <summary>An RPC request delivered to a local worker, decoded once on receipt.</summary>
/// <param name="CorrelationId">The call's correlation UUID in network byte order.</param>
/// <param name="Route">The concrete route the caller invoked.</param>
/// <param name="Body">The request body.</param>
/// <param name="Budget">The caller's remaining budget when the broker forwarded the call, if it set one.</param>
sealed record RpcInboundRequest(byte[] CorrelationId, string Route, byte[] Body, TimeSpan? Budget)
{
    /// <summary>The correlation id as a <see cref="Guid"/>, for local bookkeeping.</summary>
    internal Guid InvocationId { get; } = RpcWire.GuidFromNetworkBytes(CorrelationId);
}

/// <summary>Encoding and decoding for the RPC wire format, free of client state.</summary>
static class RpcWire
{
    internal const int CorrelationIdLength = 16;
    internal const byte ResponseFlagStreamEnd = 0x01;

    /// <summary>The wire carries a remaining budget as u32 milliseconds; the broker caps it at one day.</summary>
    internal static readonly TimeSpan MaxCallBudget = TimeSpan.FromDays(1);

    static readonly WireFailureFactory<RpcException> Failure = static failure =>
        new RpcException(failure.Message, failure.Code, failure.Status, failure.DomainCode)
        {
            ErrorClass = failure.ErrorClass,
            ErrorEffect = failure.ErrorEffect,
        };

    // Correlation ids travel as RFC 9562 UUIDs in network (big-endian) byte order.
    internal static byte[] GuidToNetworkBytes(Guid value)
    {
        var bytes = new byte[CorrelationIdLength];
        value.TryWriteBytes(bytes, bigEndian: true, out _);
        return bytes;
    }

    internal static Guid GuidFromNetworkBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != CorrelationIdLength)
            throw new ProtocolException("RPC correlation UUID must contain 16 bytes.");
        return new Guid(bytes, bigEndian: true);
    }

    /// <summary>Reads the correlation id at the start of a frame, if the frame is long enough.</summary>
    /// <param name="payload">An RPC request or response frame.</param>
    /// <returns>The correlation id, or <see langword="null"/> for a truncated frame.</returns>
    internal static Guid? PeekCorrelationId(ReadOnlyMemory<byte> payload) =>
        payload.Length < CorrelationIdLength ? null : GuidFromNetworkBytes(payload.Span[..CorrelationIdLength]);

    /// <summary>Decodes an inbound RPC request.</summary>
    /// <param name="payload">The REQUEST frame payload.</param>
    /// <returns>The request, or <see langword="null"/> when the frame is malformed.</returns>
    internal static RpcInboundRequest? TryDecodeRequest(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            if (reader.RemainingBytes < CorrelationIdLength)
                return null;

            var correlationId = reader.ReadBytes(CorrelationIdLength);
            var route = reader.ReadString();
            if (reader.RemainingBytes < 4)
                return null;

            var bodyLength = reader.ReadU32();
            if (reader.RemainingBytes < bodyLength)
                return null;

            var body = reader.ReadBytes(bodyLength);
            if (reader.RemainingBytes < 1)
                return null;

            switch (reader.ReadU8())
            {
                case 0:
                    return reader.IsEof ? new RpcInboundRequest(correlationId, route, body, null) : null;
                case 1:
                    if (reader.RemainingBytes != 4)
                        return null;
                    var budgetMs = reader.ReadU32();
                    return budgetMs <= MaxCallBudget.TotalMilliseconds
                        ? new RpcInboundRequest(correlationId, route, body, TimeSpan.FromMilliseconds(budgetMs))
                        : null;
                default:
                    return null;
            }
        }
        catch (FitzException)
        {
            return null;
        }
    }

    /// <summary>Encodes an outbound RPC request.</summary>
    internal static BinaryBufferWriter EncodeRequest(byte[] correlationId, string route, ReadOnlySpan<byte> body, TimeSpan? remainingBudget)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteBytes(correlationId);
        writer.WriteString(route);
        writer.WriteLengthPrefixed(body);
        if (remainingBudget is { } remaining)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)Math.Clamp((long)remaining.TotalMilliseconds, 0L, (long)MaxCallBudget.TotalMilliseconds));
        }
        else
        {
            writer.WriteU8(0);
        }

        return writer;
    }

    /// <summary>Decodes a terminal error carried in the body of a stream-end response frame.</summary>
    /// <param name="body">The final frame's body.</param>
    /// <returns>The error, or <see langword="null"/> when the body is ordinary data.</returns>
    internal static RpcException? TryDecodeTerminalError(byte[] body)
    {
        ResponseErrorEnvelope error;
        try
        {
            error = ResponseError.Read(body);
        }
        catch (FitzException)
        {
            return null;
        }

        return new RpcException(
            string.IsNullOrWhiteSpace(error.Message) ? "RPC error" : error.Message,
            MapErrorCode(error.Code),
            1,
            error.Code)
        {
            ErrorClass = error.Class,
            ErrorEffect = error.Effect,
        };
    }

    /// <summary>Encodes a terminal error for the body of a stream-end response frame.</summary>
    internal static byte[] EncodeTerminalError(uint code, string message, byte errorClass = 6, byte effect = 1)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU16(checked((ushort)code));
        writer.WriteU8(errorClass);
        writer.WriteU8(effect);
        writer.WriteString(message);
        return writer.Build();
    }

    /// <summary>Reads a REGISTER or UNREGISTER response, throwing the broker's error.</summary>
    internal static byte[] ReadSuccess(ReadOnlyMemory<byte> response, string operation)
    {
        var reader = ResponseEnvelope.ReadSuccess(response, operation, Failure, MapErrorCode);
        var body = reader.ReadLengthPrefixed();
        if (!reader.IsEof)
            throw new ProtocolException($"{operation} response has trailing bytes.");

        return body;
    }

    internal static RpcCancellationOutcome? MapCancellationStatus(byte status) => status switch
    {
        1 => RpcCancellationOutcome.QueuedRemoved,
        2 => RpcCancellationOutcome.Forwarded,
        3 => RpcCancellationOutcome.WorkerUnsupported,
        4 => RpcCancellationOutcome.AlreadyTerminal,
        5 => RpcCancellationOutcome.UnknownOrUnauthorized,
        6 => RpcCancellationOutcome.ForwardingFailed,
        _ => null,
    };

    internal static string MapErrorCode(uint code) => code switch
    {
        FitzErrorCodes.RpcTimeout => "TIMEOUT",
        FitzErrorCodes.RpcWorkerNotFound => "WORKER_NOT_FOUND",
        FitzErrorCodes.RpcBackpressure => "BUSY",
        FitzErrorCodes.RpcRouteNotRegistered => "ROUTE_NOT_REGISTERED",
        FitzErrorCodes.RpcCorrelationNotFound => "CORRELATION_NOT_FOUND",
        FitzErrorCodes.RpcInvalidSequence => "INVALID_SEQUENCE",
        FitzErrorCodes.RpcDuplicateCorrelation => "DUPLICATE_CORRELATION",
        FitzErrorCodes.RpcWrongWorker => "WRONG_WORKER",
        FitzErrorCodes.RpcUnauthorized => "UNAUTHORIZED",
        FitzErrorCodes.RpcBackendError => "BACKEND_ERROR",
        FitzErrorCodes.RpcInvalidRoute => "INVALID_ROUTE",
        FitzErrorCodes.RpcInvalidSubscriptionPattern => "INVALID_SUBSCRIPTION_PATTERN",
        FitzErrorCodes.RpcSubscriptionLimit => "SUBSCRIPTION_LIMIT",
        _ => "DOMAIN_ERROR",
    };
}
