namespace Cntryl.Fitz;

/// <summary>
/// One page of registered schedules.
/// </summary>
/// <param name="Entries">The schedules on this page.</param>
/// <param name="TotalCount">Compatibility count field; Wire 2 LIST does not include a total.</param>
/// <param name="HasMore">Whether a later cursor page is available.</param>
/// <param name="Continuation">Cursor for the next page, when available.</param>
public sealed record ScheduleListPage(
    IReadOnlyList<ScheduleEntry> Entries,
    ulong? TotalCount,
    bool HasMore = false,
    string? Continuation = null);
