namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILease
{
    string Route { get; }
    ulong Token { get; }
    Task ExtendAsync(ulong ttlSecs, CancellationToken cancellationToken = default);
    Task RenewAsync(ulong ttlSecs, CancellationToken cancellationToken = default);
    Task ReleaseAsync(CancellationToken cancellationToken = default);
}