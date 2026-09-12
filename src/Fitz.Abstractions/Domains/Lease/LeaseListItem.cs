namespace Cntryl.Fitz;

/// <summary>One lease entry returned by a Lease LIST scan.</summary>
public sealed record LeaseListItem(
    string Route,
    string OwnerId,
    ulong HolderIncarnation,
    string AcquiredAt,
    TimeSpan ExpiresIn,
    uint Renewals);
