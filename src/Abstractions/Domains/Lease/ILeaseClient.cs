namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILeaseClient
{
    Task<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken ct = default);
    Task<T> WithLeaseAsync<T>(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);
    Task WithLeaseAsync(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);
    Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default);
    Task<LeaseSubscription> SubscribeAsync(
        string route,
        CancellationToken ct = default);
}
