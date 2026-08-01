namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public interface IScheduleClient
{
    Task<string?> CreateAsync(string route, string cron, ScheduleDeliveryMode deliveryMode, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    Task CancelAsync(string route, CancellationToken ct = default);
    Task<ScheduleListResult> ListAsync(ulong offset = 0, ulong limit = 0, CancellationToken ct = default);
    Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default);
}
