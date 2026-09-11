namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed record ScheduleListPage(
    IReadOnlyList<ScheduleEntry> Entries,
    ulong TotalCount);
