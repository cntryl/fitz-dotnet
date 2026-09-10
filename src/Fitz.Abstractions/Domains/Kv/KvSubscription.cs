using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Kv;

public sealed class KvSubscription : SubscriptionHandle<KvNotification>
{
    public KvSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(pattern, EmptyNotifications(), unsubscribe, completion)
    {
    }

    public KvSubscription(string pattern, IAsyncEnumerable<KvNotification> notifications, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : base(pattern, notifications, unsubscribe, completion)
    {
    }
}
