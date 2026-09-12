using System.Buffers.Binary;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Protocol;

/// <summary>
/// Encodes and decodes Fitz wire frames.
/// </summary>
static class FrameCodec
{
    /// <summary>Size in bytes of a frame header: a 16-bit opcode plus a length prefix.</summary>
    public const int MaxHeaderSize = 5;

    const byte ExtendedMessageTypeMarker = 0xFF;

    /// <summary>Largest encoded size for a payload of the given length.</summary>
    /// <param name="payloadLength">Payload length in bytes.</param>
    /// <returns>The encoded frame size, header included.</returns>
    public static int MaxEncodedSize(int payloadLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(payloadLength);

        return checked(MaxHeaderSize + payloadLength);
    }

    /// <summary>Encodes a frame into a caller-supplied buffer, without allocating.</summary>
    /// <param name="messageType">Opcode from <see cref="MessageTypes"/>.</param>
    /// <param name="payload">Payload to encode.</param>
    /// <param name="destination">Buffer to write into; at least <see cref="MaxEncodedSize"/> bytes.</param>
    /// <returns>Number of bytes written.</returns>
    public static int EncodeInto(ushort messageType, ReadOnlySpan<byte> payload, Span<byte> destination)
    {
        if (payload.Length > ushort.MaxValue)
        {
            throw new ProtocolException($"Payload length {payload.Length} exceeds the 65535-byte Fitz wire limit.");
        }

        var typeLength = GetTypeLength(messageType);
        var required = typeLength + 2 + payload.Length;
        if (destination.Length < required)
        {
            throw new ArgumentException("Destination too small.", nameof(destination));
        }

        var offset = 0;
        if (typeLength == 1)
        {
            destination[offset++] = (byte)messageType;
        }
        else
        {
            destination[offset++] = ExtendedMessageTypeMarker;
            destination[offset++] = (byte)(messageType >> 8);
            destination[offset++] = (byte)(messageType & 0xFF);
        }

        BinaryPrimitives.WriteUInt16BigEndian(destination.Slice(offset, 2), (ushort)payload.Length);
        offset += 2;
        payload.CopyTo(destination.Slice(offset));
        return required;
    }

    /// <summary>Encodes a frame into a new array.</summary>
    /// <param name="messageType">Opcode from <see cref="MessageTypes"/>.</param>
    /// <param name="payload">Payload to encode.</param>
    /// <returns>The encoded frame.</returns>
    /// <remarks>Prefer <see cref="EncodeInto"/> on hot paths; this allocates.</remarks>
    public static byte[] Encode(ushort messageType, ReadOnlySpan<byte> payload)
    {
        if (payload.Length > ushort.MaxValue)
        {
            throw new ProtocolException($"Payload length {payload.Length} exceeds the 65535-byte Fitz wire limit.");
        }

        var output = GC.AllocateUninitializedArray<byte>(GetTypeLength(messageType) + 2 + payload.Length);
        EncodeInto(messageType, payload, output);
        return output;
    }

    /// <summary>
    /// Length of a <c>CORRELATE</c>/<c>CORRELATED</c> record: a 1-byte type, a 2-byte length, and an
    /// 8-byte big-endian identifier.
    /// </summary>
    public const int CorrelationRecordSize = 11;

    /// <summary>
    /// Largest legal transport frame: a correlation record followed by an extended-type record
    /// carrying the maximum 16-bit payload.
    /// </summary>
    internal const int MaxTransportFrameSize = CorrelationRecordSize + MaxHeaderSize + ushort.MaxValue;

    /// <summary>
    /// Encodes a <c>CORRELATE</c> record immediately followed by the request it labels, as the two
    /// records of a single transport frame.
    /// </summary>
    /// <remarks>
    /// The spec requires the label to precede its request in the same transport frame, so the two
    /// records are built together and handed to the transport as one send.
    /// </remarks>
    public static byte[] EncodeCorrelated(ulong correlationId, ushort messageType, ReadOnlySpan<byte> payload)
    {
        if (correlationId == 0)
        {
            throw new ProtocolException("Correlation identifier zero is reserved and must not be sent.");
        }

        if (payload.Length > ushort.MaxValue)
        {
            throw new ProtocolException($"Payload length {payload.Length} exceeds the 65535-byte Fitz wire limit.");
        }

        var requestLength = GetTypeLength(messageType) + 2 + payload.Length;
        var output = GC.AllocateUninitializedArray<byte>(CorrelationRecordSize + requestLength);

        output[0] = (byte)MessageTypes.Correlate;
        BinaryPrimitives.WriteUInt16BigEndian(output.AsSpan(1, 2), sizeof(ulong));
        BinaryPrimitives.WriteUInt64BigEndian(output.AsSpan(3, 8), correlationId);
        EncodeInto(messageType, payload, output.AsSpan(CorrelationRecordSize));
        return output;
    }

    /// <summary>
    /// Reads the identifier from a <c>CORRELATE</c>/<c>CORRELATED</c> record payload.
    /// </summary>
    public static ulong ReadCorrelationId(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != sizeof(ulong))
        {
            throw new ProtocolException($"Correlation record carries {payload.Length} bytes; expected 8.");
        }

        var correlationId = BinaryPrimitives.ReadUInt64BigEndian(payload);
        if (correlationId == 0)
        {
            throw new ProtocolException("Correlation identifier zero is reserved.");
        }

        return correlationId;
    }

    /// <summary>Decodes a frame, rejecting anything malformed.</summary>
    /// <param name="frameBytes">The encoded frame.</param>
    /// <returns>The decoded frame.</returns>
    /// <exception cref="Errors.ProtocolException">
    /// The frame is truncated, over-long, or its length prefix disagrees with its content.
    /// </exception>
    public static Frame DecodeStrict(ReadOnlyMemory<byte> frameBytes)
    {
        var span = frameBytes.Span;
        ReadHeader(span, out var messageType, out var payloadLength, out var headerLength);

        var expectedLength = headerLength + payloadLength;
        if (span.Length < expectedLength)
        {
            throw new ProtocolException("Frame payload is incomplete.");
        }

        if (span.Length != expectedLength)
        {
            throw new ProtocolException("Frame has trailing bytes.");
        }

        return new Frame(messageType, frameBytes.Slice(headerLength, payloadLength));
    }

    internal static bool TryReadHeader(ReadOnlySpan<byte> frameBytes, out ushort messageType, out ushort payloadLength, out int headerLength)
    {
        if (frameBytes.Length < 3)
        {
            messageType = default;
            payloadLength = default;
            headerLength = default;
            return false;
        }

        if (frameBytes[0] == ExtendedMessageTypeMarker)
        {
            if (frameBytes.Length < MaxHeaderSize)
            {
                messageType = default;
                payloadLength = default;
                headerLength = default;
                return false;
            }

            messageType = BinaryPrimitives.ReadUInt16BigEndian(frameBytes.Slice(1, 2));
            payloadLength = BinaryPrimitives.ReadUInt16BigEndian(frameBytes.Slice(3, 2));
            headerLength = MaxHeaderSize;
            return true;
        }

        messageType = frameBytes[0];
        payloadLength = BinaryPrimitives.ReadUInt16BigEndian(frameBytes.Slice(1, 2));
        headerLength = 3;
        return true;
    }

    static int GetTypeLength(ushort messageType) => messageType <= 0xFE ? 1 : 3;

    static void ReadHeader(ReadOnlySpan<byte> frameBytes, out ushort messageType, out ushort payloadLength, out int headerLength)
    {
        if (TryReadHeader(frameBytes, out messageType, out payloadLength, out headerLength))
        {
            return;
        }

        throw frameBytes.Length < 3
            ? new ProtocolException("Frame is too short.")
            : new ProtocolException("Extended frame header is incomplete.");
    }
}
