namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public interface IScheduleClient
{
    Task<string?> CreateAsync(string route, string cron, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    Task CancelAsync(string route, CancellationToken ct = default);
    Task<(ScheduleEntry[] Entries, ulong TotalCount)> ListAsync(ulong offset = 0, ulong limit = 0, CancellationToken ct = default);
    IAsyncEnumerable<ScheduleExecutionEvent> SubscribeAsync(string pattern, CancellationToken ct = default);
}
