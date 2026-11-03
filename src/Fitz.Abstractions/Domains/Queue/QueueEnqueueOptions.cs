namespace Cntryl.Fitz;

/// <summary>Optional Queue publication controls. Persistence remains best effort.</summary>
/// <param name="Delay">Initial visibility delay in whole seconds.</param>
/// <param name="TtlHours">Positive per-message lifetime in hours, requiring negotiated support.</param>
public sealed record QueueEnqueueOptions(TimeSpan? Delay = null, uint? TtlHours = null);
