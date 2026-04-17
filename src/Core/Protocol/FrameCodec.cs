using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

public static class FrameCodec
{
    public const int MaxHeaderSize = 5;

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

        var typeLength = messageType <= 0xFE ? 1 : 3;
        var required = typeLength + 2 + payload.Length;
        if (destination.Length < required)
        {
            throw new ArgumentException("Destination too small.", nameof(destination));
        }

        var offset = 0;
        if (messageType <= 0xFE)
        {
            destination[offset++] = (byte)messageType;
        }
        else
        {
            destination[offset++] = 0xFF;
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

        var typeLength = messageType <= 0xFE ? 1 : 3;
        var output = GC.AllocateUninitializedArray<byte>(typeLength + 2 + payload.Length);
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
        if (span.Length < 3)
        {
            throw new InvalidOperationException("Frame is too short.");
        }

        var offset = 0;
        ushort messageType;
        var first = span[offset++];
        if (first == 0xFF)
        {
            if (span.Length < 5)
            {
                throw new InvalidOperationException("Extended frame header is incomplete.");
            }

            messageType = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
            offset += 2;
        }
        else
        {
            messageType = first;
        }

        var payloadLength = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(offset, 2));
        offset += 2;
        if (payloadLength > ushort.MaxValue)
        {
            throw new InvalidOperationException("Frame payload exceeds allowed length.");
        }

        var expectedLength = offset + payloadLength;
        if (span.Length < expectedLength)
        {
            throw new InvalidOperationException("Frame payload is incomplete.");
        }

        if (strict && span.Length != expectedLength)
        {
            throw new InvalidOperationException("Frame has trailing bytes.");
        }

        return new Frame(messageType, frameBytes.Slice(offset, payloadLength));
    }
}