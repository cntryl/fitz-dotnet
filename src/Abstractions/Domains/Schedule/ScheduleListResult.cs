namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed record ScheduleListResult(IReadOnlyList<ScheduleEntry> Entries, ulong TotalCount);
