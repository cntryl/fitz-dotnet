using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

public static class FrameCodec
{
    public const int MaxHeaderSize = 5;

    private const byte ExtendedMessageTypeMarker = 0xFF;

    public static int MaxEncodedSize(int payloadLength)
    {
        if (payloadLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(payloadLength));
        }

        return checked(MaxHeaderSize + payloadLength);
    }

    public static int EncodeInto(ushort messageType, ReadOnlySpan<byte> payload, Span<byte> destination)
    {
        if (payload.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(payload), "Payload exceeds frame limit.");
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

    public static byte[] Encode(ushort messageType, ReadOnlySpan<byte> payload)
    {
        if (payload.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(payload), "Payload exceeds frame limit.");
        }

        var output = GC.AllocateUninitializedArray<byte>(GetTypeLength(messageType) + 2 + payload.Length);
        EncodeInto(messageType, payload, output);
        return output;
    }

    public static Frame DecodeStrict(ReadOnlyMemory<byte> frameBytes)
    {
        return Decode(frameBytes, strict: true);
    }

    public static Frame Decode(ReadOnlyMemory<byte> frameBytes, bool strict = false)
    {
        var span = frameBytes.Span;
        ReadHeader(span, out var messageType, out var payloadLength, out var headerLength);

        var expectedLength = headerLength + payloadLength;
        if (span.Length < expectedLength)
        {
            throw new InvalidOperationException("Frame payload is incomplete.");
        }

        if (strict && span.Length != expectedLength)
        {
            throw new InvalidOperationException("Frame has trailing bytes.");
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

    private static int GetTypeLength(ushort messageType)
    {
        return messageType <= 0xFE ? 1 : 3;
    }

    private static void ReadHeader(ReadOnlySpan<byte> frameBytes, out ushort messageType, out ushort payloadLength, out int headerLength)
    {
        if (TryReadHeader(frameBytes, out messageType, out payloadLength, out headerLength))
        {
            return;
        }

        throw frameBytes.Length < 3
            ? new InvalidOperationException("Frame is too short.")
            : new InvalidOperationException("Extended frame header is incomplete.");
    }
}