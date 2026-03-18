namespace Cntryl.Fitz.Abstractions;

using Cntryl.Fitz.Abstractions.Domains.Kv;

public interface IClient : IAsyncDisposable
{
    Task ConnectAsync(CancellationToken cancellationToken = default);
    Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default);
    IKvClient Kv();
}