namespace Cntryl.Fitz.Abstractions;

using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Abstractions.Domains.Rpc;

public interface IClient : IAsyncDisposable
{
    Task ConnectAsync(CancellationToken cancellationToken = default);
    Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default);
    IKvClient Kv();
    IQueueClient Queue();
    IRpcClient Rpc();
}