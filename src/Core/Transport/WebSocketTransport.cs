using System.Net.WebSockets;

namespace Cntryl.Fitz.Transport;

public sealed class WebSocketTransport : ITransport
{
    private readonly Uri _uri;
    private readonly TimeSpan _timeout;
    private ClientWebSocket? _socket;

    public WebSocketTransport(string url, TimeSpan timeout)
    {
        _uri = new Uri(url);
        _timeout = timeout;
    }

    public string Url => _uri.ToString();

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_socket is { State: WebSocketState.Open })
        {
            return;
        }

        _socket = new ClientWebSocket();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_timeout);
        await _socket.ConnectAsync(_uri, cts.Token);
    }

    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var socket = EnsureSocket();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_timeout);
        await socket.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, cts.Token);
    }

    public async Task<int> ReceiveAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var socket = EnsureSocket();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_timeout);
        var result = await socket.ReceiveAsync(buffer, cts.Token);
        return result.Count;
    }

    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        var socket = _socket;
        _socket = null;

        if (socket is null)
        {
            return;
        }

        if (socket.State == WebSocketState.Open)
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "client closing", cancellationToken);
        }

        socket.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
    }

    private ClientWebSocket EnsureSocket()
    {
        if (_socket is null || _socket.State != WebSocketState.Open)
        {
            throw new InvalidOperationException("Transport is not connected.");
        }

        return _socket;
    }
}