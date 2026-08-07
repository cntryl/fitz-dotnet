namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public sealed record LeaseInfo(
    bool IsHeld,
    string? Owner = null,
    ulong? TtlRemainingSecs = null,
    uint PendingWaiters = 0);
