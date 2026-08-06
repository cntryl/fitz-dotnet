namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public interface IScheduleClient
{
    Task<string?> CreateAsync(string route, string cron, ScheduleDeliveryMode deliveryMode, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    Task CancelAsync(string route, CancellationToken ct = default);
    Task<ScheduleListPage> ListPageAsync(string? cursor = null, ulong? limit = null, CancellationToken ct = default);
    Task<IReadOnlyList<ScheduleEntry>> ListBySelectorAsync(string selector, CancellationToken ct = default);
    Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default);
}
