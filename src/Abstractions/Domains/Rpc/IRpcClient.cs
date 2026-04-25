namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

public interface IRpcClient
{
    IAsyncEnumerable<RpcResponseFrame> CallAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);
    Task<RpcWorkerRegistration> RegisterWorkerAsync(string pattern, Func<RpcRequest, IRpcResponseWriter, CancellationToken, Task> handler, CancellationToken ct = default);
}
