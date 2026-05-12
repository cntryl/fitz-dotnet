namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILease
{
    string Route { get; }
    Task ExtendAsync(ulong ttlSecs, CancellationToken ct = default);
    Task ReleaseAsync(CancellationToken ct = default);
}
