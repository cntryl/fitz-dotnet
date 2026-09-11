namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>
/// A point-in-time view of a lease's ownership, as reported by <c>ILeaseClient.QueryAsync</c>.
/// </summary>
/// <param name="IsHeld">Whether the lease was held when the broker answered.</param>
/// <param name="Owner">Identifier of the current holder, when the broker discloses one.</param>
/// <param name="TtlRemainingSecs">Seconds remaining before the current hold expires.</param>
/// <param name="PendingWaiters">Number of sessions waiting to acquire the lease.</param>
/// <remarks>
/// This is a snapshot, not a reservation. Ownership can change immediately after the query
/// returns; use a fencing token to gate side effects rather than acting on this value.
/// </remarks>
public sealed record LeaseInfo(
    bool IsHeld,
    string? Owner = null,
    ulong? TtlRemainingSecs = null,
    uint PendingWaiters = 0);
