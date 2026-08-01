using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Kv;

public sealed class KvSubscription : SubscriptionHandle
{
    public KvSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, unsubscribe)
    {
    }
}
