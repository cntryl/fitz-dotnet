namespace Cntryl.Fitz;

/// <summary>
/// One cursor page of registered schedules.
/// </summary>
/// <param name="Entries">The schedules on this page.</param>
/// <param name="HasMore">Whether another page is available.</param>
/// <param name="Continuation">Cursor for the next page; set exactly when <paramref name="HasMore"/> is true.</param>
public sealed record ScheduleListPage(
    IReadOnlyList<ScheduleEntry> Entries,
    bool HasMore,
    string? Continuation);
