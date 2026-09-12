namespace Cntryl.Fitz;

/// <summary>
/// Continuation state for a paged stream read.
/// </summary>
/// <param name="LastResourceOffset">Last resource-scoped position covered by the page.</param>
/// <param name="LastAreaOffset">Last area-scoped position, when the broker reports one.</param>
/// <param name="LastRealmOffset">Last realm-scoped position, when the broker reports one.</param>
/// <param name="LastGlobalOffset">Last global position, when the broker reports one.</param>
/// <param name="CursorFingerprint">
/// Opaque value that validates this cursor against the stream it came from. Pass it back
/// unmodified; a mismatched cursor is rejected rather than silently reinterpreted.
/// </param>
/// <param name="CapturedWatermark">Stream watermark captured when the read began.</param>
/// <param name="HasMore">Whether more records follow this page.</param>
public sealed record StreamReadCursor(
    ulong LastResourceOffset,
    ulong? LastAreaOffset,
    ulong? LastRealmOffset,
    ulong? LastGlobalOffset,
    ulong? CursorFingerprint,
    ulong? CapturedWatermark,
    bool HasMore);
