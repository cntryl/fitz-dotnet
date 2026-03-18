namespace Cntryl.Fitz.Abstractions.Domains.Schedule;

public interface IScheduleClient
{
    Task<string?> CreateAsync(string route, string cron, byte[] payload, CancellationToken cancellationToken = default);
    Task CancelAsync(string route, CancellationToken cancellationToken = default);
}