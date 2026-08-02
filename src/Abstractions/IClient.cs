namespace Cntryl.Fitz.Abstractions;

using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz;

public interface IClient : IAsyncDisposable
{
    Task ConnectAsync(CancellationToken cancellationToken = default);
    Task ConnectWhenReadyAsync(ConnectWhenReadyOptions? options = null, CancellationToken cancellationToken = default);
    Task CloseAsync(CancellationToken cancellationToken = default);
    bool IsConnected { get; }
    ConnectionState State { get; }
    IKvClient Kv { get; }
    ILeaseClient Lease { get; }
    INoticeClient Notice { get; }
    IQueueClient Queue { get; }
    IRpcClient Rpc { get; }
    IScheduleClient Schedule { get; }
    IStreamClient Stream { get; }
}
