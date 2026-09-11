namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamReadCursor(
    ulong LastResourceOffset,
    ulong? LastAreaOffset,
    ulong? LastRealmOffset,
    ulong? LastGlobalOffset,
    ulong? CursorFingerprint,
    ulong? CapturedWatermark,
    bool HasMore);
