using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed class ScheduleSubscription : SubscriptionHandle
{
    public ScheduleSubscription(
        ulong subscriptionId,
        string pattern,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(subscriptionId, pattern, unsubscribe)
    {
    }
}