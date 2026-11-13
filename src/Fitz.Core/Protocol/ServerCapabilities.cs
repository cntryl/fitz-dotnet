using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

/// <summary>Limits and session data negotiated by the Wire 3 HELLO frame.</summary>
sealed record ServerCapabilities(
    ushort ProtocolVersion,
    uint CapabilityBits,
    uint MaxMessageBytes,
    uint MaxInFlightRequests,
    ulong SessionId)
{
    internal uint CapabilityMask { get; init; }
    internal bool SupportsCapability(uint id) => id is 1 or 2 && (CapabilityMask & (1u << (int)id)) != 0;
    public static ServerCapabilities None { get; } = new(0, 0, 1024 * 1024, 256, 0);

    /// <summary>Parses the frozen HELLO core; optional values belong in registered extensions.</summary>
    public static bool TryParse(ReadOnlySpan<byte> payload, out ServerCapabilities capabilities)
    {
        if (payload.Length != 22)
        {
            capabilities = None;
            return false;
        }
        capabilities = new ServerCapabilities(
            BinaryPrimitives.ReadUInt16BigEndian(payload[..2]),
            BinaryPrimitives.ReadUInt32BigEndian(payload.Slice(2, 4)),
            BinaryPrimitives.ReadUInt32BigEndian(payload.Slice(6, 4)),
            BinaryPrimitives.ReadUInt32BigEndian(payload.Slice(10, 4)),
            BinaryPrimitives.ReadUInt64BigEndian(payload.Slice(14, 8)));
        return capabilities.ProtocolVersion == 3 && capabilities.CapabilityBits == 0 && capabilities.MaxMessageBytes >= 15 && capabilities.MaxMessageBytes <= FrameCodec.MaxTransportFrameSize && capabilities.MaxInFlightRequests > 0;
    }
}
