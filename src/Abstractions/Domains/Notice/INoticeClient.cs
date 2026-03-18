namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public interface INoticeClient
{
    Task PublishAsync(string route, byte[] body, CancellationToken cancellationToken = default);
}