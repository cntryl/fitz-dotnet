using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public sealed class NoticeSubscription : SubscriptionHandle<NoticeMessage>
{
    public NoticeSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(pattern, EmptyNotifications(), unsubscribe, completion)
    {
    }

    public NoticeSubscription(
        string pattern, IAsyncEnumerable<NoticeMessage> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(pattern, notifications, unsubscribe, completion)
    {
    }
}
