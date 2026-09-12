namespace Cntryl.Fitz;

/// <summary>
/// Acquires distributed leases and observes their ownership.
/// </summary>
/// <remarks>
/// Prefer the <c>WithLeaseAsync</c> overloads: they own acquisition, renewal, callback
/// cancellation, and release, and they cancel the callback token the moment ownership is
/// lost. <see cref="AcquireAsync"/> returns a low-level handle whose lifecycle you manage.
/// </remarks>
public interface ILeaseClient
{
    /// <summary>
    /// Claims a lease and returns a handle you are responsible for renewing and releasing.
    /// </summary>
    /// <param name="route">Exact <c>lease://realm/area/resource</c> route. Patterns are not accepted.</param>
    /// <param name="ttl">Initial time-to-live. Must be a whole number of seconds.</param>
    /// <param name="wait">
    /// How long to wait for a contended lease before failing. Zero fails immediately.
    /// </param>
    /// <param name="ct">Cancellation token for the acquisition.</param>
    /// <returns>A handle holding the lease.</returns>
    Task<ILease> AcquireAsync(string route, TimeSpan ttl, TimeSpan wait = default, CancellationToken ct = default);

    /// <summary>
    /// Runs a callback while holding a lease, returning its result.
    /// </summary>
    /// <typeparam name="T">Result type produced by the callback.</typeparam>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttl">Lease time-to-live; renewal is automatic. Must be a whole number of seconds.</param>
    /// <param name="callback">
    /// Work to run under the lease. Its token is cancelled as soon as ownership is lost, and
    /// the callback must honor it promptly.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>The callback's result.</returns>
    /// <remarks>
    /// Defaults to the authority-aware overload, discarding the fence. Implement only that
    /// overload; this one needs no implementation of its own.
    /// </remarks>
    Task<T> WithLeaseAsync<T>(
        string route,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(callback);
        return WithLeaseAsync(route, ttl, (_, callbackToken) => callback(callbackToken), options, ct);
    }

    /// <summary>
    /// Runs an authority-aware callback while holding a lease, returning its result.
    /// </summary>
    /// <typeparam name="T">Result type produced by the callback.</typeparam>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttl">Lease time-to-live; renewal is automatic. Must be a whole number of seconds.</param>
    /// <param name="callback">
    /// Work to run under the lease, receiving the admission fence from the successful acquire.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>The callback's result.</returns>
    Task<T> WithLeaseAsync<T>(
        string route,
        TimeSpan ttl,
        Func<LeaseAuthority, CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);

    /// <summary>
    /// Runs a callback while holding a lease.
    /// </summary>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttl">Lease time-to-live; renewal is automatic. Must be a whole number of seconds.</param>
    /// <param name="callback">
    /// Work to run under the lease. Its token is cancelled as soon as ownership is lost.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>A task that completes when the callback finishes and the lease is released.</returns>
    /// <remarks>
    /// Defaults to the authority-aware overload, discarding the fence. Implement only that
    /// overload; this one needs no implementation of its own.
    /// </remarks>
    Task WithLeaseAsync(
        string route,
        TimeSpan ttl,
        Func<CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(callback);
        return WithLeaseAsync(route, ttl, (_, callbackToken) => callback(callbackToken), options, ct);
    }

    /// <summary>
    /// Runs an authority-aware callback while holding a lease.
    /// </summary>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttl">Lease time-to-live; renewal is automatic. Must be a whole number of seconds.</param>
    /// <param name="callback">
    /// Work to run under the lease, receiving the admission fence from the successful acquire.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>A task that completes when the callback finishes and the lease is released.</returns>
    Task WithLeaseAsync(
        string route,
        TimeSpan ttl,
        Func<LeaseAuthority, CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);

    /// <summary>
    /// Reads the current holder and expiry of a lease without claiming it.
    /// </summary>
    /// <param name="route">Exact lease route to inspect.</param>
    /// <param name="ct">Cancellation token for the query.</param>
    /// <returns>The lease's current ownership state.</returns>
    Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default);

    /// <summary>
    /// Subscribes to ownership changes for one lease.
    /// </summary>
    /// <param name="route">
    /// Exact <c>lease://realm/area/resource</c> route. Unlike other domains, lease
    /// subscriptions do not accept patterns.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>
    /// A handle that yields change events by <c>await foreach</c> and unsubscribes on disposal.
    /// </returns>
    Task<LeaseSubscription> SubscribeAsync(
        string route,
        CancellationToken ct = default);

    /// <summary>
    /// Reads one page of leases matching a pattern.
    /// </summary>
    /// <param name="pattern">Route or whole-segment pattern to match.</param>
    /// <param name="cursor">Continuation cursor from a previous page, or <see langword="null"/> to start.</param>
    /// <param name="limit">Maximum entries to return. Defaults to the broker's page size.</param>
    /// <param name="ct">Cancellation token for the list request.</param>
    /// <returns>The page of entries and the cursor for the next page, if any.</returns>
    Task<LeaseListResult> ListAsync(
        string pattern,
        LeaseListCursor? cursor = null,
        int? limit = null,
        CancellationToken ct = default);

    /// <summary>
    /// Starts a race-safe, high-level observer over every lease matching <paramref name="pattern"/>.
    /// See <see cref="ILeaseInventoryObserver"/> for the guarantees it provides.
    /// </summary>
    /// <param name="pattern">Route or whole-segment pattern to observe.</param>
    /// <param name="options">Observer options, including reconciliation behavior.</param>
    /// <param name="ct">Cancellation token for the bootstrap.</param>
    /// <returns>An observer that converges on the current inventory and streams updates.</returns>
    Task<ILeaseInventoryObserver> ObserveAsync(
        string pattern,
        LeaseObserveOptions? options = null,
        CancellationToken ct = default);
}
