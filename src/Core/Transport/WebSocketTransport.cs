using System.Buffers;
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
        await _socket.ConnectAsync(_uri, cts.Token).ConfigureAwait(false);
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

    public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var socket = EnsureSocket();

        var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
        var length = 0;
        var ownsBuffer = true;
        while (true)
        {
            var remaining = buffer.Length - length;
            if (remaining == 0)
            {
                var next = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                buffer.AsSpan(0, length).CopyTo(next);
                ArrayPool<byte>.Shared.Return(buffer);
                buffer = next;
                remaining = buffer.Length - length;
            }

            var result = await socket.ReceiveAsync(buffer.AsMemory(length, remaining), cancellationToken).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                if (ownsBuffer)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }

                return PooledFrame.Closed;
            }

            length += result.Count;
            if (result.EndOfMessage)
            {
                if (length == 0)
                {
                    if (ownsBuffer)
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }

                    return PooledFrame.Empty;
                }

                ownsBuffer = false;
                return PooledFrame.FromRentedBuffer(buffer, length);
            }
        }
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
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "client closing", cancellationToken).ConfigureAwait(false);
        }

        socket.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
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
