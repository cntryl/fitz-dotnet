namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public interface INoticeClient
{
    ValueTask PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default);
    Task<NoticeSubscription> SubscribeAsync(string pattern, Func<NoticeMessage, CancellationToken, ValueTask> handler, CancellationToken ct = default);
}