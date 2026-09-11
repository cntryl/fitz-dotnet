namespace Cntryl.Fitz.Abstractions.Domains.Lease;

/// <summary>
/// A held lease. Dispose to release it; disposal is best-effort and bounded.
/// </summary>
/// <remarks>
/// Prefer <c>ILeaseClient.WithLeaseAsync</c>, which owns renewal and release for you. Use a
/// low-level handle only when you need to control that lifecycle yourself. A handle closes
/// itself when renewal outcome is uncertain, so ownership is never silently assumed.
/// </remarks>
public interface ILease : IAsyncDisposable
{
    /// <summary>The exact lease route this handle holds.</summary>
    string Route { get; }

    /// <summary>The immutable fencing token issued for this lease generation.</summary>
    ulong FencingToken { get; }

    /// <summary>
    /// Extends the lease before it expires.
    /// </summary>
    /// <param name="ttlSecs">New time-to-live in seconds, measured from broker acceptance.</param>
    /// <param name="ct">Cancellation token for the renewal.</param>
    /// <returns>A task that completes once the broker accepts the renewal.</returns>
    Task ExtendAsync(ulong ttlSecs, CancellationToken ct = default);

    /// <summary>
    /// Releases the lease so another owner may claim it.
    /// </summary>
    /// <param name="ct">Cancellation token for the release.</param>
    /// <returns>A task that completes once the broker accepts the release.</returns>
    Task ReleaseAsync(CancellationToken ct = default);
}
