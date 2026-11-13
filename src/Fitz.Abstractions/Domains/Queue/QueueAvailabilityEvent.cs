namespace Cntryl.Fitz;

/// <summary>
/// Message counts for a queue matched by a subscription, reported when they change.
/// </summary>
/// <param name="Route">Concrete queue route the counts describe.</param>
/// <param name="Ready">Messages available to reserve now.</param>
/// <param name="Delayed">Messages waiting for their delivery delay to elapse.</param>
/// <param name="InFlight">Messages reserved and awaiting completion.</param>
public sealed record QueueAvailabilityEvent(
    string Route,
    ulong Ready,
    ulong Delayed,
    ulong InFlight);
