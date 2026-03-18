namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILeaseClient
{
    Task<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken ct = default);
    Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default);
    IAsyncEnumerable<LeaseChangeEvent> SubscribeAsync(string pattern, CancellationToken ct = default);
}