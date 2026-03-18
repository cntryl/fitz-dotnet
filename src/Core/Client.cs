using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz;

public sealed class Client : IClient
{
    private readonly ClientConfig _config;
    private readonly FitzConnection _connection;

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

    public bool IsConnected => _connection.State == ConnectionState.Authenticated;
}