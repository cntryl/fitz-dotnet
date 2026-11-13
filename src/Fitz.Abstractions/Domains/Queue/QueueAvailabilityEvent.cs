namespace Cntryl.Fitz;

/// <summary>
/// Queue notification received for a subscribed route pattern. Payload contents are broker-defined.
/// </summary>
/// <remarks>
/// Wire 3 payloads contain three big-endian <see cref="ulong"/> counters in order:
/// ready, delayed, and inflight messages (24 bytes total), without a length prefix.
/// </remarks>
public sealed record QueueAvailabilityEvent(
    string Route,
    ReadOnlyMemory<byte> Payload);
