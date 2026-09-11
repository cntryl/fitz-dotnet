namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

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
    /// The response frames in order, ending with the terminal frame. A unary call yields a
    /// single frame. Enumeration is lazy: no request is sent until enumeration starts.
    /// </returns>
    IAsyncEnumerable<RpcResponseFrame> CallAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);

    /// <summary>
    /// Registers a handler that serves invocations for routes matching a pattern.
    /// </summary>
    /// <param name="pattern">
    /// An exact route or a whole-segment <c>*</c>/<c>**</c> pattern. RPC patterns have
    /// flexible depth. Wildcard patterns consume the broker's per-domain quota.
    /// </param>
    /// <param name="handler">
    /// Invoked for each inbound request. Write the response through the supplied writer;
    /// the handler's cancellation token is cancelled when the registration ends or the
    /// connection is lost.
    /// </param>
    /// <param name="options">Worker options, including concurrency. Defaults to one at a time.</param>
    /// <param name="ct">Cancellation token for the registration request.</param>
    /// <returns>A registration that withdraws the worker when disposed.</returns>
    Task<RpcWorkerRegistration> RegisterWorkerAsync(string pattern, Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> handler, RpcWorkerOptions? options = null, CancellationToken ct = default);
}
