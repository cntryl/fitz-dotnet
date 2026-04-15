using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed class StreamSubscription : SubscriptionHandle
{
    public StreamSubscription(
        ulong subscriptionId,
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(subscriptionId, pattern, unsubscribe)
    {
    }
}