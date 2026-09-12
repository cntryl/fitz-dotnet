namespace Cntryl.Fitz;

/// <summary>
/// Queue notification received for a subscribed route pattern. Payload contents are broker-defined.
/// </summary>
public sealed record QueueAvailabilityEvent(
    string Route,
    ReadOnlyMemory<byte> Payload);
