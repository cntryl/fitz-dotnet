namespace Cntryl.Fitz.Protocol;

/// <summary>
/// A decoded Fitz frame.
/// </summary>
/// <param name="MessageType">Opcode from <see cref="MessageTypes"/>.</param>
/// <param name="Payload">Frame payload, excluding the header.</param>
readonly record struct Frame(ushort MessageType, ReadOnlyMemory<byte> Payload);
