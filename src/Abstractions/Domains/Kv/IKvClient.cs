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
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A transaction handle used for KV operations.</returns>
    Task<IKvTransaction> BeginAsync(
        string route,
        KvDurability durability,
        KvMode mode = KvMode.ReadWrite,
        CancellationToken cancellationToken = default
    );

    Task<KvSubscription> SubscribeAsync(
        string pattern,
        CancellationToken cancellationToken = default
    );
}
