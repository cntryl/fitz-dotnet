namespace Cntryl.Fitz.Protocol;

public readonly record struct Frame(ushort MessageType, byte[] Payload);