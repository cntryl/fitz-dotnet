namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamRecord(ulong Offset, byte[] Body);