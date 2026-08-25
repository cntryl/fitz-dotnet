using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed class StreamSubscription : SubscriptionHandle<StreamCommitEvent>
{
    public StreamSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(pattern, EmptyNotifications(), unsubscribe, completion)
    {
    }

    public StreamSubscription(
        string pattern, IAsyncEnumerable<StreamCommitEvent> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(pattern, notifications, unsubscribe, completion)
    {
    }
}
