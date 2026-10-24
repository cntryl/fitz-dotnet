using System.Buffers.Binary;

namespace Cntryl.Fitz.Protocol;

/// <summary>Limits and session data negotiated by the Wire-2 HELLO frame.</summary>
sealed record ServerCapabilities(
    ushort ProtocolVersion,
    uint CapabilityBits,
    uint MaxMessageBytes,
    uint MaxInFlightRequests,
    ulong SessionId)
{
    public const uint CorrelationBit = 0;
    public const uint SessionMetadataBit = 0;
    public const uint KvScanExclusiveBit = 0;
    public const uint RpcCancellationBit = 0;
    public static ServerCapabilities None { get; } = new(0, 0, 1024 * 1024, 256, 0);
    public bool SupportsCorrelation => ProtocolVersion >= 2;
    public bool SupportsSessionMetadata => false;
    public bool SupportsKvScanExclusive => ProtocolVersion >= 2;
    public bool SupportsRpcCancellation => ProtocolVersion >= 2;

    /// <summary>Parses the fixed HELLO fields and permits forward-compatible trailing bytes.</summary>
    public static bool TryParse(ReadOnlySpan<byte> payload, out ServerCapabilities capabilities)
    {
        if (payload.Length < 22)
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
        return capabilities.ProtocolVersion == 2 && capabilities.MaxMessageBytes >= 15 && capabilities.MaxMessageBytes <= FrameCodec.MaxTransportFrameSize && capabilities.MaxInFlightRequests > 0;
    }
}
