using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

/// <summary>Encodes and decodes Wire-2 Fitz frames.</summary>
static class FrameCodec
{
    public const int MaxHeaderSize = 15;
    internal const int MaxTransportFrameSize = 1024 * 1024;
    // Kept as an internal source-compatibility name; correlation now occupies the frame header.
    public const int CorrelationRecordSize = MaxHeaderSize;
    public const byte AckRequestedFlag = 1;
    public const byte ResponseFinalFlag = 2;
    const byte KnownFlags = AckRequestedFlag | ResponseFinalFlag;

    /// <summary>Largest encoded size for a payload of the given length.</summary>
    public static int MaxEncodedSize(int payloadLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(payloadLength);
        var encodedLength = checked(MaxHeaderSize + payloadLength);
        if (encodedLength > MaxTransportFrameSize)
        {
            throw new ProtocolException($"Encoded frame length {encodedLength} exceeds the 1 MiB Wire-2 limit.");
        }
        return encodedLength;
    }

    /// <summary>Encodes a frame into a caller-supplied buffer without allocating.</summary>
    public static int EncodeInto(
        ushort messageType,
        ReadOnlySpan<byte> payload,
        Span<byte> destination,
        byte flags = 0,
        ulong correlationId = 0)
    {
        ValidateFlags(flags);
        var required = MaxEncodedSize(payload.Length);
        if (destination.Length < required)
        {
            throw new ArgumentException("Destination too small.", nameof(destination));
        }

        BinaryPrimitives.WriteUInt16BigEndian(destination[..2], messageType);
        destination[2] = flags;
        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(3, 8), correlationId);
        BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(11, 4), (uint)payload.Length);
        payload.CopyTo(destination[MaxHeaderSize..]);
        return required;
    }

    /// <summary>Encodes a frame into a new array.</summary>
    public static byte[] Encode(ushort messageType, ReadOnlySpan<byte> payload, byte flags = 0, ulong correlationId = 0)
    {
        var output = GC.AllocateUninitializedArray<byte>(MaxEncodedSize(payload.Length));
        EncodeInto(messageType, payload, output, flags, correlationId);
        return output;
    }

    internal static byte[] EncodeCorrelated(ulong correlationId, ushort messageType, ReadOnlySpan<byte> payload)
    {
        if (correlationId == 0)
        {
            throw new ProtocolException("Correlation identifier zero is reserved for notifications.");
        }
        return Encode(messageType, payload, correlationId: correlationId);
    }

    internal static ulong ReadCorrelationId(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != sizeof(ulong))
        {
            throw new ProtocolException($"Correlation value carries {payload.Length} bytes; expected 8.");
        }
        return BinaryPrimitives.ReadUInt64BigEndian(payload);
    }

    /// <summary>Decodes one complete frame, rejecting truncation and trailing bytes.</summary>
    public static Frame DecodeStrict(ReadOnlyMemory<byte> frameBytes)
    {
        if (!TryReadHeader(frameBytes.Span, out var messageType, out var flags, out var correlationId, out var payloadLength))
        {
            throw new ProtocolException("Frame header is incomplete.");
        }

        var expectedLength = checked(MaxHeaderSize + (int)payloadLength);
        if (frameBytes.Length < expectedLength)
        {
            throw new ProtocolException("Frame payload is incomplete.");
        }
        if (frameBytes.Length != expectedLength)
        {
            throw new ProtocolException("Frame has trailing bytes.");
        }

        return new Frame(messageType, flags, correlationId, frameBytes[MaxHeaderSize..]);
    }

    internal static bool TryReadHeader(
        ReadOnlySpan<byte> frameBytes,
        out ushort messageType,
        out byte flags,
        out ulong correlationId,
        out uint payloadLength)
    {
        if (frameBytes.Length < MaxHeaderSize)
        {
            messageType = default;
            flags = default;
            correlationId = default;
            payloadLength = default;
            return false;
        }

        messageType = BinaryPrimitives.ReadUInt16BigEndian(frameBytes[..2]);
        flags = frameBytes[2];
        ValidateFlags(flags);
        correlationId = BinaryPrimitives.ReadUInt64BigEndian(frameBytes.Slice(3, 8));
        payloadLength = BinaryPrimitives.ReadUInt32BigEndian(frameBytes.Slice(11, 4));
        if (payloadLength > MaxTransportFrameSize - MaxHeaderSize)
        {
            throw new ProtocolException($"Payload length {payloadLength} exceeds the 1 MiB Wire-2 frame limit.");
        }
        return true;
    }

    static void ValidateFlags(byte flags)
    {
        if ((flags & ~KnownFlags) != 0)
        {
            throw new ProtocolException($"Frame contains unknown flags 0x{flags:X2}.");
        }
    }
}
