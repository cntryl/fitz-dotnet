namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

/// <summary>
/// How each firing of a schedule is delivered to its subscribers.
/// </summary>
public enum ScheduleDeliveryMode : byte
{
    /// <summary>Deliver every firing to every current subscriber.</summary>
    Broadcast = 0,

    /// <summary>Deliver each firing to exactly one subscriber.</summary>
    Single = 1,
}
