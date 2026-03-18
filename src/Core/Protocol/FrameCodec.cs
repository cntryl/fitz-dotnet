namespace Cntryl.Fitz.Protocol;

public static class FrameCodec
{
    public static byte[] Encode(ushort messageType, ReadOnlySpan<byte> payload)
    {
        if (payload.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(payload), "Payload exceeds frame limit.");
        }

        var typeLength = messageType <= 0xFE ? 1 : 3;
        var output = GC.AllocateUninitializedArray<byte>(typeLength + 2 + payload.Length);
        var offset = 0;

        if (messageType <= 0xFE)
        {
            output[offset++] = (byte)messageType;
        }
        else
        {
            output[offset++] = 0xFF;
            output[offset++] = (byte)(messageType >> 8);
            output[offset++] = (byte)(messageType & 0xFF);
        }

        output[offset++] = (byte)(payload.Length >> 8);
        output[offset++] = (byte)(payload.Length & 0xFF);
        payload.CopyTo(output.AsSpan(offset));
        return output;
    }

    public static Frame Decode(ReadOnlySpan<byte> frameBytes)
    {
        if (frameBytes.Length < 3)
        {
            throw new InvalidOperationException("Frame is too short.");
        }

        var offset = 0;
        ushort messageType;
        var first = frameBytes[offset++];
        if (first == 0xFF)
        {
            if (frameBytes.Length < 5)
            {
                throw new InvalidOperationException("Extended frame header is incomplete.");
            }

            messageType = (ushort)((frameBytes[offset++] << 8) | frameBytes[offset++]);
        }
        else
        {
            messageType = first;
        }

        var payloadLength = (frameBytes[offset++] << 8) | frameBytes[offset++];
        if (payloadLength > ushort.MaxValue)
        {
            throw new InvalidOperationException("Frame payload exceeds allowed length.");
        }

        if (frameBytes.Length - offset < payloadLength)
        {
            throw new InvalidOperationException("Frame payload is incomplete.");
        }

        var payload = frameBytes.Slice(offset, payloadLength).ToArray();
        return new Frame(messageType, payload);
    }
}