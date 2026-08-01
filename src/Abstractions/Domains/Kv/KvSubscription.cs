using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Kv;

public sealed class KvSubscription : SubscriptionHandle<KvNotification>
{
    public KvSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe)
        : this(pattern, EmptyNotifications(), unsubscribe)
    {
    }

    public KvSubscription(string pattern, IAsyncEnumerable<KvNotification> notifications, Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, notifications, unsubscribe)
    {
    }
}
