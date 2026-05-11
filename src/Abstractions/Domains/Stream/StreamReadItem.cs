namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamReadItem(
    StreamReadItemKind Kind,
    StreamRecord? Record = null,
    ulong Offset = 0,
    ulong FromOffset = 0,
    ulong ToOffset = 0,
    StreamFilteredReason? Reason = null);
