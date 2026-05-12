using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz;

public sealed class Client : IClient
{
    private readonly ClientConfig _config;
    private readonly FitzConnection _connection;
    private KvClient? _kvClient;
    private LeaseClient? _leaseClient;
    private NoticeClient? _noticeClient;
    private QueueClient? _queueClient;
    private RpcClient? _rpcClient;
    private ScheduleClient? _scheduleClient;
    private StreamClient? _streamClient;

    public Client(ClientConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        var transportFactory = _config.TransportFactory ?? TransportResolver.Resolve;
        _connection = new FitzConnection(_config, () => transportFactory(_config));
    }

    public ClientConfig Config => _config;

    public Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        return _connection.ConnectAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.CloseAsync();
    }

    public IKvClient Kv()
    {
        return _kvClient ??= new KvClient(_connection);
    }

    public ILeaseClient Lease()
    {
        return _leaseClient ??= new LeaseClient(_connection);
    }

    public INoticeClient Notice()
    {
        return _noticeClient ??= new NoticeClient(_connection);
    }

    public IQueueClient Queue()
    {
        return _queueClient ??= new QueueClient(_connection);
    }

    public IRpcClient Rpc()
    {
        return _rpcClient ??= new RpcClient(_connection);
    }

    public IScheduleClient Schedule()
    {
        return _scheduleClient ??= new ScheduleClient(_connection);
    }

    public IStreamClient Stream()
    {
        return _streamClient ??= new StreamClient(_connection);
    }

    public bool IsConnected => _connection.State == ConnectionState.Authenticated;

    public ConnectionState State => _connection.State;

}
