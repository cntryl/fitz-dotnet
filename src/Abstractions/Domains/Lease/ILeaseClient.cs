namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILeaseClient
{
    ValueTask<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken ct = default);
    ValueTask<LeaseInfo> QueryAsync(string route, CancellationToken ct = default);
    Task<LeaseSubscription> SubscribeAsync(
        string pattern,
        Func<LeaseChangeEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default);
}