namespace Cntryl.Fitz.Protocol;

/// <summary>
/// Broker capabilities advertised by a <c>SERVER_HELLO</c> (4) frame.
/// </summary>
/// <remarks>
/// A broker that predates the advertisement never sends one. Absence means "legacy broker" and is
/// never an error: the client keeps one in-flight request per message type and sends no
/// <c>CORRELATE</c> records.
/// </remarks>
public readonly record struct ServerCapabilities(ushort ProtocolVersion, uint CapabilityBits)
{
    /// <summary>Bit 0: the broker accepts <c>CORRELATE</c> and echoes <c>CORRELATED</c>.</summary>
    public const uint CorrelationBit = 1u << 0;

    /// <summary>The state before any advertisement arrives, and the state for a legacy broker.</summary>
    public static ServerCapabilities None => default;

    /// <summary>Whether per-request correlation may be used on this session.</summary>
    public bool SupportsCorrelation => (CapabilityBits & CorrelationBit) != 0;

    /// <summary>
    /// Parses a <c>SERVER_HELLO</c> payload: <c>[u16 BE protocol_version][u32 BE capability_bits]</c>.
    /// </summary>
    /// <remarks>
    /// Trailing bytes are ignored so a later protocol version can append fields without consuming
    /// another message id. A truncated payload is ignored rather than rejected, because an
    /// unparseable advertisement must not be more disruptive than a missing one.
    /// </remarks>
    public static bool TryParse(ReadOnlySpan<byte> payload, out ServerCapabilities capabilities)
    {
        if (payload.Length < 6)
        {
            capabilities = None;
            return false;
        }

        capabilities = new ServerCapabilities(
            System.Buffers.Binary.BinaryPrimitives.ReadUInt16BigEndian(payload[..2]),
            System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(payload.Slice(2, 4)));
        return true;
    }
}
