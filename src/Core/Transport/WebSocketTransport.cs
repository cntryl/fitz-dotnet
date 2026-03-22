using System.Net.WebSockets;

namespace Cntryl.Fitz.Transport;

public sealed class WebSocketTransport : ITransport
{
    private readonly Uri _uri;
    private readonly TimeSpan _timeout;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
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
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_timeout);
            await socket.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, cts.Token).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async Task<byte[]> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var socket = EnsureSocket();

        using var stream = new MemoryStream();
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return [];
            }

            if (result.Count > 0)
            {
                stream.Write(buffer, 0, result.Count);
            }

            if (result.EndOfMessage)
            {
                break;
            }
        }

        return stream.ToArray();
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
