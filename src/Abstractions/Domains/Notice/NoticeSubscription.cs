using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public sealed class NoticeSubscription : SubscriptionHandle<NoticeMessage>
{
    public NoticeSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe)
        : this(pattern, EmptyNotifications(), unsubscribe)
    {
    }

    public NoticeSubscription(
        string pattern, IAsyncEnumerable<NoticeMessage> notifications,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, notifications, unsubscribe)
    {
    }
}
