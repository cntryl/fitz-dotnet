namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed record ScheduleEntry(string Id, string Route, string Cron, ScheduleDeliveryMode DeliveryMode, byte[] Payload);
