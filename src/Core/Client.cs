using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz;

public sealed class Client : IClient
{
    private readonly ClientConfig _config;
    private readonly FitzConnection _connection;
    private KvClient? _kvClient;
    private QueueClient? _queueClient;
    private RpcClient? _rpcClient;

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

    public Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default)
    {
        return _connection.RequestAsync(messageType, payload, cancellationToken);
    }

    public Task SendAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default)
    {
        return _connection.SendAsync(messageType, payload, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.CloseAsync();
    }

    public IKvClient Kv()
    {
        return _kvClient ??= new KvClient(_connection);
    }

    public IQueueClient Queue()
    {
        return _queueClient ??= new QueueClient(_connection);
    }

    public IRpcClient Rpc()
    {
        return _rpcClient ??= new RpcClient(_connection);
    }

    public bool IsConnected => _connection.State == ConnectionState.Authenticated;
}