namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public interface INoticeClient
{
    Task PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);
    IAsyncEnumerable<NoticeMessage> SubscribeAsync(string pattern, CancellationToken ct = default);
}