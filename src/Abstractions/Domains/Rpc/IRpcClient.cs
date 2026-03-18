namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

public interface IRpcClient
{
    IAsyncEnumerable<RpcResponseFrame> CallAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);
    Task RegisterWorkerAsync(string pattern, Func<RpcRequest, Task< IRpcResponseWriter>> handler, CancellationToken ct = default);
}