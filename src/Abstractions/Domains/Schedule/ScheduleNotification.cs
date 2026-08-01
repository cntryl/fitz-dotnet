namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed record ScheduleNotification(string Route, ReadOnlyMemory<byte> Payload);
