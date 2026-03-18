namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILease
{
    string Route { get; }
    ulong Token { get; }
    Task ExtendAsync(ulong ttlSecs, CancellationToken ct = default);
    Task ReleaseAsync(CancellationToken ct = default);
}