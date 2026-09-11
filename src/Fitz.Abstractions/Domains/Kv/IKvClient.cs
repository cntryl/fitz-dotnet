namespace Cntryl.Fitz.Abstractions.Domains.Kv;

/// <summary>
/// Provides entry points for starting KV transactions.
/// </summary>
public interface IKvClient
{
    /// <summary>
    /// Begins a KV transaction for a route.
    /// </summary>
    /// <param name="route">Target KV route.</param>
    /// <param name="durability">Requested durability level.</param>
    /// <param name="mode">Transaction mode.</param>
    /// <param name="ct">Cancellation token for the operation.</param>
    /// <returns>A transaction handle used for KV operations.</returns>
    Task<IKvTransaction> BeginAsync(
        string route,
        KvDurability durability,
        KvMode mode = KvMode.ReadWrite,
        CancellationToken ct = default
    );

    /// <summary>
    /// Subscribes to key changes on routes matching a pattern.
    /// </summary>
    /// <param name="pattern">
    /// An exact <c>kv://realm/area/resource</c> route, or a whole-segment <c>*</c>/<c>**</c>
    /// pattern capable of matching three segments. Wildcard patterns consume the broker's
    /// per-domain registration quota; exact routes do not.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>
    /// A handle that yields notifications by <c>await foreach</c> and unsubscribes on disposal.
    /// </returns>
    Task<KvSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default
    );
}
