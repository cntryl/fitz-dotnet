namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamMetadata(ulong FirstOffset, ulong LastOffset, ulong RecordCount);