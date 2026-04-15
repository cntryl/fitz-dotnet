namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed record ScheduleNotification(ReadOnlyMemory<byte> Payload);