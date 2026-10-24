namespace Cntryl.Fitz.Protocol;

/// <summary>A decoded Wire-2 Fitz frame.</summary>
/// <param name="MessageType">Opcode from <see cref="MessageTypes"/>.</param>
/// <param name="Flags">Frame flags; bit 0 requests an acknowledgement.</param>
/// <param name="CorrelationId">Request/response correlation identifier, or zero for notifications.</param>
/// <param name="Payload">Frame payload, excluding the fixed header.</param>
readonly record struct Frame(ushort MessageType, byte Flags, ulong CorrelationId, ReadOnlyMemory<byte> Payload);
