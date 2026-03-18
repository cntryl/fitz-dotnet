namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILeaseClient
{
    Task<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken cancellationToken = default);
    Task<LeaseInfo> QueryAsync(string route, CancellationToken cancellationToken = default);
}