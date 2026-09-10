using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public sealed class ScheduleSubscription : SubscriptionHandle<ScheduleNotification>
{
    public ScheduleSubscription(string pattern, Func<CancellationToken, ValueTask> unsubscribe, Task? completion = null)
        : this(pattern, EmptyNotifications(), unsubscribe, completion)
    {
    }

    public ScheduleSubscription(
        string pattern, IAsyncEnumerable<ScheduleNotification> notifications,
        Func<CancellationToken, ValueTask> unsubscribe,
        Task? completion = null)
        : base(pattern, notifications, unsubscribe, completion)
    {
    }
}
