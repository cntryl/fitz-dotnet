namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILeaseClient
{
    const string AuthorityCallbacksNotSupportedMessage =
        "This ILeaseClient implementation does not support managed lease authority callbacks.";

    Task<ILease> AcquireAsync(string route, ulong ttlSecs, uint waitSeconds = 0, CancellationToken ct = default);
    Task<T> WithLeaseAsync<T>(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);
    Task<T> WithLeaseAsync<T>(
        string route,
        ulong ttlSecs,
        Func<LeaseAuthority, CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(callback);
        throw new NotSupportedException(AuthorityCallbacksNotSupportedMessage);
    }
    Task WithLeaseAsync(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);
    Task WithLeaseAsync(
        string route,
        ulong ttlSecs,
        Func<LeaseAuthority, CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(callback);
        throw new NotSupportedException(AuthorityCallbacksNotSupportedMessage);
    }
    Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default);
    Task<LeaseSubscription> SubscribeAsync(
        string route,
        CancellationToken ct = default);
}
