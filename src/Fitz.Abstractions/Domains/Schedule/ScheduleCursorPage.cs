namespace Cntryl.Fitz;

/// <summary>A cursor page returned by broker Schedule LIST_V2 (707).</summary>
/// <param name="Entries">Schedules on this page.</param>
/// <param name="HasMore">Whether another page is available.</param>
/// <param name="Continuation">Cursor for the next page, if supplied.</param>
public sealed record ScheduleCursorPage(
    IReadOnlyList<ScheduleEntry> Entries,
    bool HasMore,
    string? Continuation);
