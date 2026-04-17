namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public interface IScheduleClient
{
    ValueTask<string?> CreateAsync(string route, string cron, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    ValueTask CancelAsync(string route, CancellationToken ct = default);
    Task<(ScheduleEntry[] Entries, ulong TotalCount)> ListAsync(ulong offset = 0, ulong limit = 0, CancellationToken ct = default);
    Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        Func<ScheduleNotification, CancellationToken, ValueTask> handler,
        CancellationToken ct = default);
}
