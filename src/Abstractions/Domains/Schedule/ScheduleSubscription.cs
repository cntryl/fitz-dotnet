using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed class ScheduleSubscription : SubscriptionHandle<ScheduleNotification>
{
    public ScheduleSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe)
        : this(pattern, EmptyNotifications(), unsubscribe)
    {
    }

    public ScheduleSubscription(
        string pattern, IAsyncEnumerable<ScheduleNotification> notifications,
        Func<CancellationToken, ValueTask> unsubscribe)
        : base(pattern, notifications, unsubscribe)
    {
    }
}
