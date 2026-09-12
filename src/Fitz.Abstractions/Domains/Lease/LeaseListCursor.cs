namespace Cntryl.Fitz;

/// <summary>Opaque continuation token for paging through a Lease LIST scan. Pass back verbatim to continue the same scan.</summary>
public sealed record LeaseListCursor(ulong SnapshotId, uint Offset);
