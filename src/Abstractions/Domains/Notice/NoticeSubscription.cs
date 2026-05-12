using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public sealed class NoticeSubscription : SubscriptionHandle
{
    public NoticeSubscription(
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, unsubscribe)
    {
    }
}
