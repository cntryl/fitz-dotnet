namespace Cntryl.Fitz;

/// <summary>
/// A race-safe, high-level view over every lease matching a patterned selector.
/// </summary>
/// <remarks>
/// Owns the bootstrap sequence required to build a consistent view (subscribe, buffer
/// notifications, LIST to completion, install, drain), reconciles with a fresh LIST after
/// each notification so every item retains its complete opaque holder metadata, backstops with a
/// periodic full relist, and rebuilds from scratch whenever the underlying connection
/// reconnects. Dispose to stop all background work and unsubscribe.
/// </remarks>
public interface ILeaseInventoryObserver : IAsyncDisposable
{
    /// <summary>
    /// The current observed view: route to its most recently known <see cref="LeaseListItem"/>.
    /// This is a live snapshot reference that is safe to read concurrently with updates.
    /// </summary>
    IReadOnlyDictionary<string, LeaseListItem> View { get; }

    /// <summary>
    /// <see langword="true"/> once the initial bootstrap (subscribe, list, drain) has completed
    /// and <see cref="View"/> reflects a consistent snapshot. Goes back to <see langword="false"/>
    /// while a reconnect-triggered rebootstrap is in progress.
    /// </summary>
    bool IsReady { get; }

    /// <summary>
    /// Steady-state changes applied to <see cref="View"/> after the initial bootstrap. Full
    /// relists (the post-bootstrap drain, periodic reconciliation, and reconnect rebootstrap) are
    /// not individually reported here. This stream is bounded and drops its oldest pending entry
    /// when full; read <see cref="View"/> for the authoritative snapshot.
    /// </summary>
    IAsyncEnumerable<LeaseInventoryUpdate> Updates { get; }
}
