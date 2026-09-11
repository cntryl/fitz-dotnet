namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

/// <summary>
/// A single schedule firing delivered to a subscriber.
/// </summary>
/// <param name="Route">The concrete route that fired.</param>
/// <param name="Payload">The payload registered with the schedule.</param>
public sealed record ScheduleNotification(string Route, ReadOnlyMemory<byte> Payload);
