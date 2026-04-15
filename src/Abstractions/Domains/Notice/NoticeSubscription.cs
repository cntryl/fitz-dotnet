using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Notice;

public sealed class NoticeSubscription : SubscriptionHandle
{
    public NoticeSubscription(
        ulong subscriptionId,
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(subscriptionId, pattern, unsubscribe)
    {
    }
}