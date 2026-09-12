using Cntryl.Fitz;

namespace Cntryl.Fitz;

/// <summary>
/// A connection to a Fitz broker and the entry point to every domain API.
/// </summary>
/// <remarks>
/// One client owns one connection and is safe for concurrent use. Domain clients are created
/// on first access and share the connection. Dispose the client to close the connection and
/// release every subscription and registration it owns.
/// </remarks>
public interface IClient : IAsyncDisposable
{
    /// <summary>
    /// Opens the transport connection and authenticates, failing if the attempt does not succeed.
    /// </summary>
    /// <param name="ct">Cancellation token for the attempt.</param>
    /// <returns>A task that completes once the session is authenticated.</returns>
    Task ConnectAsync(CancellationToken ct = default);

    /// <summary>
    /// Connects, retrying transport failures until the deadline elapses.
    /// </summary>
    /// <param name="options">Timeout and backoff for the retry loop. Defaults are used when omitted.</param>
    /// <param name="ct">Cancellation token for the attempt.</param>
    /// <returns>A task that completes once the session is authenticated.</returns>
    /// <remarks>
    /// Prefer this at application startup when the broker may not be up yet. An authentication
    /// rejection is authoritative and is not retried.
    /// </remarks>
    Task ConnectWhenReadyAsync(ConnectWhenReadyOptions? options = null, CancellationToken ct = default);

    /// <summary>
    /// Closes the connection and releases the subscriptions and registrations it owns.
    /// </summary>
    /// <param name="ct">Cancellation token bounding the close.</param>
    /// <returns>A task that completes once the connection is closed.</returns>
    Task CloseAsync(CancellationToken ct = default);

    /// <summary>
    /// Whether the session is currently connected and authenticated.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// The current connection lifecycle state.
    /// </summary>
    ConnectionState State { get; }

    /// <summary>Key/value transactions.</summary>
    IKvClient Kv { get; }

    /// <summary>Distributed leases and leader election.</summary>
    ILeaseClient Lease { get; }

    /// <summary>Fire-and-forget notices.</summary>
    INoticeClient Notice { get; }

    /// <summary>Work queues with lease-based delivery.</summary>
    IQueueClient Queue { get; }

    /// <summary>Request/response and streaming RPC.</summary>
    IRpcClient Rpc { get; }

    /// <summary>Cron schedules.</summary>
    IScheduleClient Schedule { get; }

    /// <summary>Append-only streams.</summary>
    IStreamClient Stream { get; }
}
