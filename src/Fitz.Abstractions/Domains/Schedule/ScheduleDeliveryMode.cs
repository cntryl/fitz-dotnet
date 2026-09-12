namespace Cntryl.Fitz;

/// <summary>
/// How each firing of a schedule is delivered to its subscribers.
/// </summary>
public enum ScheduleDeliveryMode
{
    /// <summary>Deliver every firing to every current subscriber.</summary>
    Broadcast = 0,

    /// <summary>Deliver each firing to exactly one subscriber.</summary>
    Once = 1,
}
