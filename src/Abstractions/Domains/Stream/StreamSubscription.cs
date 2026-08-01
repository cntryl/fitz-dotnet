using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed class StreamSubscription : SubscriptionHandle<StreamCommitEvent>
{
    public StreamSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe)
        : this(pattern, EmptyNotifications(), unsubscribe)
    {
    }

    public StreamSubscription(
        string pattern, IAsyncEnumerable<StreamCommitEvent> notifications,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, notifications, unsubscribe)
    {
    }
}
