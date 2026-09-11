namespace Cntryl.Fitz.Abstractions.Domains.Lease;

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
    /// Message for the <see cref="NotSupportedException"/> thrown by implementations that do
    /// not support authority-aware callbacks.
    /// </summary>
    const string AuthorityCallbacksNotSupportedMessage =
        "This ILeaseClient implementation does not support managed lease authority callbacks.";

    /// <summary>
    /// Message for the <see cref="NotSupportedException"/> thrown by implementations that do
    /// not support <see cref="ListAsync"/>.
    /// </summary>
    const string ListNotSupportedMessage =
        "This ILeaseClient implementation does not support LIST.";

    /// <summary>
    /// Message for the <see cref="NotSupportedException"/> thrown by implementations that do
    /// not support <see cref="ObserveAsync"/>.
    /// </summary>
    const string ObserveNotSupportedMessage =
        "This ILeaseClient implementation does not support ObserveAsync.";

    /// <summary>
    /// Claims a lease and returns a handle you are responsible for renewing and releasing.
    /// </summary>
    /// <param name="route">Exact <c>lease://realm/area/resource</c> route. Patterns are not accepted.</param>
    /// <param name="ttlSecs">Initial time-to-live in seconds.</param>
    /// <param name="waitSeconds">
    /// How long to wait for a contended lease before failing. Zero fails immediately.
    /// </param>
    /// <param name="ct">Cancellation token for the acquisition.</param>
    /// <returns>A handle holding the lease.</returns>
    Task<ILease> AcquireAsync(string route, ulong ttlSecs, uint waitSeconds = 0, CancellationToken ct = default);

    /// <summary>
    /// Runs a callback while holding a lease, returning its result.
    /// </summary>
    /// <typeparam name="T">Result type produced by the callback.</typeparam>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttlSecs">Lease time-to-live in seconds; renewal is automatic.</param>
    /// <param name="callback">
    /// Work to run under the lease. Its token is cancelled as soon as ownership is lost, and
    /// the callback must honor it promptly.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>The callback's result.</returns>
    Task<T> WithLeaseAsync<T>(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask<T>> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);

    /// <summary>
    /// Runs an authority-aware callback while holding a lease, returning its result.
    /// </summary>
    /// <typeparam name="T">Result type produced by the callback.</typeparam>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttlSecs">Lease time-to-live in seconds; renewal is automatic.</param>
    /// <param name="callback">
    /// Work to run under the lease, receiving the admission fence from the successful acquire.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>The callback's result.</returns>
    /// <exception cref="NotSupportedException">
    /// The implementation does not support authority-aware callbacks.
    /// </exception>
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

    /// <summary>
    /// Runs a callback while holding a lease.
    /// </summary>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttlSecs">Lease time-to-live in seconds; renewal is automatic.</param>
    /// <param name="callback">
    /// Work to run under the lease. Its token is cancelled as soon as ownership is lost.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>A task that completes when the callback finishes and the lease is released.</returns>
    Task WithLeaseAsync(
        string route,
        ulong ttlSecs,
        Func<CancellationToken, ValueTask> callback,
        LeaseExecutionOptions? options = null,
        CancellationToken ct = default);

    /// <summary>
    /// Runs an authority-aware callback while holding a lease.
    /// </summary>
    /// <param name="route">Exact lease route to hold.</param>
    /// <param name="ttlSecs">Lease time-to-live in seconds; renewal is automatic.</param>
    /// <param name="callback">
    /// Work to run under the lease, receiving the admission fence from the successful acquire.
    /// </param>
    /// <param name="options">Contention behavior. Defaults to failing fast on a held lease.</param>
    /// <param name="ct">Cancellation token for the whole scope.</param>
    /// <returns>A task that completes when the callback finishes and the lease is released.</returns>
    /// <exception cref="NotSupportedException">
    /// The implementation does not support authority-aware callbacks.
    /// </exception>
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
    /// <exception cref="NotSupportedException">The implementation does not support listing.</exception>
    Task<LeaseListResult> ListAsync(
        string pattern,
        LeaseListCursor? cursor = null,
        int? limit = null,
        CancellationToken ct = default) => throw new NotSupportedException(ListNotSupportedMessage);

    /// <summary>
    /// Starts a race-safe, high-level observer over every lease matching <paramref name="pattern"/>.
    /// See <see cref="ILeaseInventoryObserver"/> for the guarantees it provides.
    /// </summary>
    /// <param name="pattern">Route or whole-segment pattern to observe.</param>
    /// <param name="options">Observer options, including reconciliation behavior.</param>
    /// <param name="ct">Cancellation token for the bootstrap.</param>
    /// <returns>An observer that converges on the current inventory and streams updates.</returns>
    /// <exception cref="NotSupportedException">The implementation does not support observation.</exception>
    Task<ILeaseInventoryObserver> ObserveAsync(
        string pattern,
        LeaseObserveOptions? options = null,
        CancellationToken ct = default) => throw new NotSupportedException(ObserveNotSupportedMessage);
}
