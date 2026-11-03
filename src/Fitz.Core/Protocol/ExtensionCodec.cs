using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

/// <summary>Wire 3 extension envelopes. Unknown values remain borrowed and are skipped.</summary>
static class ExtensionCodec
{
    internal const byte PresentFlag = 4;
    internal static ReadOnlySpan<byte> ClientCapabilities => [0, 2, 0, 0, 0, 1, 0, 0, 0, 2];

    internal static byte[] Encode(ReadOnlySpan<byte> core, ushort fieldId, ReadOnlySpan<byte> value)
    {
        ArgumentOutOfRangeException.ThrowIfZero(fieldId);
        var size = checked(4 + core.Length + 6 + value.Length);
        _ = FrameCodec.MaxEncodedSize(size);
        var result = new byte[size];
        BinaryPrimitives.WriteUInt32BigEndian(result, (uint)core.Length);
        core.CopyTo(result.AsSpan(4));
        var cursor = 4 + core.Length;
        BinaryPrimitives.WriteUInt16BigEndian(result.AsSpan(cursor), fieldId);
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(cursor + 2), (uint)value.Length);
        value.CopyTo(result.AsSpan(cursor + 6));
        return result;
    }

    internal static (ReadOnlyMemory<byte> Core, uint CapabilityMask) Decode(Frame frame)
    {
        var handshake = frame.MessageType == MessageTypes.Hello;
        if ((frame.Flags & PresentFlag) == 0)
        {
            if (handshake)
                throw new ProtocolException("HELLO requires explicit capability extensions.");
            return (frame.Payload, 0);
        }
        var payload = frame.Payload.Span;
        if (payload.Length < 4)
            throw new ProtocolException("Truncated extension core length.");
        var coreLength = BinaryPrimitives.ReadUInt32BigEndian(payload);
        if (coreLength >= (uint)(payload.Length - 4))
            throw new ProtocolException("Invalid core length or missing extensions.");
        var coreEnd = 4 + (int)coreLength;
        var cursor = coreEnd;
        var capabilitiesSeen = false;
        uint mask = 0;
        while (cursor < payload.Length)
        {
            if (payload.Length - cursor < 6)
                throw new ProtocolException("Truncated extension header.");
            var id = BinaryPrimitives.ReadUInt16BigEndian(payload[cursor..]);
            var length = BinaryPrimitives.ReadUInt32BigEndian(payload[(cursor + 2)..]);
            cursor += 6;
            if (id == 0 || length > (uint)(payload.Length - cursor))
                throw new ProtocolException("Invalid extension ID or length.");
            if (handshake && id == 1)
            {
                if (capabilitiesSeen || length < 2)
                    throw new ProtocolException("Duplicate or truncated capability list.");
                capabilitiesSeen = true;
                var count = BinaryPrimitives.ReadUInt16BigEndian(payload[cursor..]);
                if (length != 2u + 4u * count)
                    throw new ProtocolException("Capability length mismatch.");
                uint previous = 0;
                for (var index = 0; index < count; index++)
                {
                    var capability = BinaryPrimitives.ReadUInt32BigEndian(payload[(cursor + 2 + 4 * index)..]);
                    if (capability <= previous)
                        throw new ProtocolException("Capability IDs must be nonzero and ascending.");
                    previous = capability;
                    if (capability is 1 or 2)
                        mask |= 1u << (int)capability;
                }
            }
            cursor += (int)length;
        }
        if (handshake && !capabilitiesSeen)
            throw new ProtocolException("HELLO capability list is missing.");
        return (frame.Payload.Slice(4, (int)coreLength), mask);
    }
}
