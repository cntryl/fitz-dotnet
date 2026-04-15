namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>
/// Lease change notification.
/// Sent when a watched lease changes state.
/// </summary>
public sealed record LeaseChangeEvent(string Route);
