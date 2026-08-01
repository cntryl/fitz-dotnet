namespace Cntryl.Fitz.Abstractions.Domains.Queue;

/// <summary>
/// Queue availability notification (sent when messages become available).
/// Received via Subscribe on queue patterns.
/// </summary>
public sealed record QueueAvailabilityEvent(
    string Route,
    ulong ReadyMessages,
    ulong DelayedMessages,
    ulong InflightMessages);
