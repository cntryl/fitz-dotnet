namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>
/// Lease status change notification.
/// Sent when a lease is acquired, released, or extended by any holder.
/// </summary>
public sealed record LeaseChangeEvent(string Route, LeaseStatus Status);

public sealed record LeaseStatus(bool IsHeld, string? Owner = null, ulong? TtlRemainingSecs = null);
