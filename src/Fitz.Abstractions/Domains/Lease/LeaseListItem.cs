namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>One lease entry returned by a Lease LIST scan.</summary>
public sealed record LeaseListItem(
    string Route,
    string OwnerId,
    ulong HolderIncarnation,
    string AcquiredAt,
    ulong ExpiresInSecs,
    uint Renewals);
