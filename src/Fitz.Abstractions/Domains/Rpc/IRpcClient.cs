namespace Cntryl.Fitz;

/// <summary>
/// Invokes RPC routes and registers workers that serve them.
/// </summary>
public interface IRpcClient
{
    /// <summary>
    /// Invokes a route and streams the response frames a worker produces.
    /// </summary>
    /// <param name="route">Concrete <c>rpc://</c> route to invoke.</param>
    /// <param name="body">Opaque request payload.</param>
    /// <param name="ct">Cancellation token for the call.</param>
    /// <returns>
    /// A lazy response sequence and a task for the broker's cancellation result. No request
    /// is sent until enumeration starts. A unary call yields one frame.
    /// </returns>
    RpcCall CallAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);

    /// <summary>Invokes a route with an explicit end-to-end budget.</summary>
    /// <param name="route">Concrete <c>rpc://</c> route to invoke.</param>
    /// <param name="body">Opaque request payload.</param>
    /// <param name="timeout">End-to-end budget propagated to supporting workers.</param>
    /// <param name="ct">Cancellation token for the call.</param>
    /// <returns>A lazy response sequence and a task for the broker's cancellation result.</returns>
    RpcCall CallAsync(string route, ReadOnlyMemory<byte> body, TimeSpan? timeout, CancellationToken ct = default);

    /// <summary>
    /// Registers a handler that serves invocations for routes matching a pattern.
    /// </summary>
    /// <param name="pattern">
    /// An exact route or a whole-segment <c>*</c>/<c>**</c> pattern. RPC patterns have
    /// flexible depth. Wildcard patterns consume the broker's per-domain quota.
    /// </param>
    /// <param name="handler">
    /// Invoked for each inbound request. Write the response through the supplied writer;
    /// the handler's cancellation token is cancelled when the broker requests cancellation
    /// or the connection is lost. Use <see cref="RpcRequest.RemainingTime"/> when passing
    /// the remaining budget to a downstream RPC call.
    /// </param>
    /// <param name="options">Worker options, including concurrency. Defaults to one at a time.</param>
    /// <param name="ct">Cancellation token for the registration request.</param>
    /// <returns>A registration that withdraws the worker when disposed.</returns>
    Task<RpcWorkerRegistration> RegisterWorkerAsync(string pattern, Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> handler, RpcWorkerOptions? options = null, CancellationToken ct = default);
}
