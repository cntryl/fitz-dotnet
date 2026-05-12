using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed class StreamSubscription : SubscriptionHandle
{
    public StreamSubscription(
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, unsubscribe)
    {
    }
}
