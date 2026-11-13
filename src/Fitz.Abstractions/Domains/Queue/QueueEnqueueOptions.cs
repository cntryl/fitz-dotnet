namespace Cntryl.Fitz;

/// <summary>Optional Queue publication controls. Persistence remains best effort.</summary>
/// <param name="Delay">Initial visibility delay. Must be a whole number of seconds.</param>
/// <param name="TimeToLive">
/// Per-message lifetime, after which an undelivered message is discarded. Must be a positive
/// whole number of hours and requires broker support for the queue TTL capability.
/// </param>
public sealed record QueueEnqueueOptions(TimeSpan? Delay = null, TimeSpan? TimeToLive = null);
