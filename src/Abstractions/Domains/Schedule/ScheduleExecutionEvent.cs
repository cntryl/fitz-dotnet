namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

/// <summary>
/// Schedule job execution notification.
/// Sent when a scheduled job executes according to its cron expression.
/// </summary>
public sealed record ScheduleExecutionEvent(string ScheduleId, string Route, ulong ExecutedAtMs);
