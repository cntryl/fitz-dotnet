namespace Cntryl.Fitz;

/// <summary>
/// One page of registered schedules.
/// </summary>
/// <param name="Entries">The schedules on this page.</param>
/// <param name="TotalCount">Total schedules registered, across every page.</param>
public sealed record ScheduleListPage(
    IReadOnlyList<ScheduleEntry> Entries,
    ulong TotalCount);
