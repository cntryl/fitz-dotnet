namespace Cntryl.Fitz.Abstractions;

public interface IClient : IAsyncDisposable
{
    Task ConnectAsync(CancellationToken cancellationToken = default);
    Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default);
}