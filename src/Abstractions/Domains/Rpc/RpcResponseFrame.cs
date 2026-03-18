namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

public sealed record RpcResponseFrame(byte[] Body, ulong Sequence);