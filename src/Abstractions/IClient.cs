namespace Cntryl.Fitz.Abstractions;

using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Abstractions.Domains.Stream;

public interface IClient : IAsyncDisposable
{
    Task ConnectAsync(CancellationToken cancellationToken = default);
    Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default);
    IKvClient Kv();
    ILeaseClient Lease();
    INoticeClient Notice();
    IQueueClient Queue();
    IRpcClient Rpc();
    IScheduleClient Schedule();
    IStreamClient Stream();
}