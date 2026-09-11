namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

/// <summary>A schedule returned by LIST.</summary>
/// <param name="Id">Broker schedule identifier, or <see langword="null"/> when the LIST wire response does not include one.</param>
/// <param name="Route">Concrete schedule route.</param>
/// <param name="Cron">Cron expression.</param>
/// <param name="DeliveryMode">Delivery mode.</param>
/// <param name="Payload">Owned schedule payload.</param>
public sealed record ScheduleEntry(string? Id, string Route, string Cron, ScheduleDeliveryMode DeliveryMode, byte[] Payload);
