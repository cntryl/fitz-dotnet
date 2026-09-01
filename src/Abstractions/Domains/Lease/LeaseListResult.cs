namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>One page of a Lease LIST scan.</summary>
public sealed record LeaseListResult(
    IReadOnlyList<LeaseListItem> Items,
    LeaseListCursor? NextCursor);
