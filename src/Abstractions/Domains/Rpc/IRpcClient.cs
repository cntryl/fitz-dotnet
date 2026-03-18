namespace Cntryl.Fitz.Abstractions.Domains.Rpc;

public interface IRpcClient
{
    Task RequestAsync(string route, byte[] body, CancellationToken cancellationToken = default);
}