namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamReadCursor(
    ulong LastResourceOffset,
    ulong? LastAreaOffset,
    ulong? LastRealmOffset,
    bool HasMore);
