namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public interface INoticeClient
{
    Task PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);
    Task<NoticeSubscription> SubscribeAsync(string pattern, Func<NoticeMessage, CancellationToken, ValueTask> handler, CancellationToken ct = default);
}