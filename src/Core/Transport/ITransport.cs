namespace Cntryl.Fitz.Transport;

public interface ITransport : IAsyncDisposable
{
    string Url { get; }
    Task ConnectAsync(CancellationToken cancellationToken = default);
    Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
    ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default);
    Task CloseAsync(CancellationToken cancellationToken = default);
}