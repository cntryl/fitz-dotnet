using System.Buffers;
using System.Net.WebSockets;

namespace Cntryl.Fitz.Transport;

public sealed class WebSocketTransport : ITransport
{
    private readonly Uri _uri;
    private readonly TimeSpan _timeout;
    private readonly int _maxFrameSize;
    private readonly WebSocketOptions? _options;
    private readonly HeartbeatOptions _heartbeat;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private ClientWebSocket? _socket;

    public WebSocketTransport(
        Uri url,
        TimeSpan timeout,
        int maxFrameSize,
        WebSocketOptions? options = null,
        HeartbeatOptions? heartbeat = null)
    {
        ArgumentNullException.ThrowIfNull(url);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxFrameSize);

        _uri = url;
        _timeout = timeout;
        _maxFrameSize = maxFrameSize;
        _options = options;
        _heartbeat = heartbeat ?? new HeartbeatOptions();
    }

    public Uri Url => _uri;

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_socket is { State: WebSocketState.Open })
        {
            return;
        }

        _socket = new ClientWebSocket();
        if (_heartbeat.Enabled)
        {
            _socket.Options.KeepAliveInterval = _heartbeat.Interval ?? TimeSpan.FromSeconds(10);
        }

        if (_options?.Headers is not null)
        {
            foreach (var header in _options.Headers)
            {
                _socket.Options.SetRequestHeader(header.Key, header.Value);
            }
        }

        using var timeoutCts = new CancellationTokenSource(_timeout);
        using var cancellationRegistration = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), timeoutCts)
            : default;
        await _socket.ConnectAsync(_uri, timeoutCts.Token).ConfigureAwait(false);
    }

    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var socket = EnsureSocket();
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var timeoutCts = new CancellationTokenSource(_timeout);
            using var cancellationRegistration = cancellationToken.CanBeCanceled
                ? cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), timeoutCts)
                : default;
            await socket.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, timeoutCts.Token).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var socket = EnsureSocket();

        var initialBufferSize = Math.Min(16 * 1024, _maxFrameSize);
        var buffer = ArrayPool<byte>.Shared.Rent(initialBufferSize);
        var length = 0;
        var ownsBuffer = true;
        try
        {
            while (true)
            {
                var remaining = buffer.Length - length;
                if (remaining == 0)
                {
                    if (buffer.Length >= _maxFrameSize)
                    {
                        throw new InvalidOperationException($"WebSocket frame length exceeds max frame size {_maxFrameSize}.");
                    }

                    var nextSize = Math.Min(buffer.Length * 2, _maxFrameSize);
                    var next = ArrayPool<byte>.Shared.Rent(nextSize);
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
                EnsureBinaryMessage(result.MessageType);

                length += result.Count;
                if (length > _maxFrameSize)
                {
                    throw new InvalidOperationException($"WebSocket frame length exceeds max frame size {_maxFrameSize}.");
                }

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
        catch
        {
            if (ownsBuffer)
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            throw;
        }
    }

    internal static void EnsureBinaryMessage(WebSocketMessageType messageType)
    {
        if (messageType == WebSocketMessageType.Text)
        {
            throw new InvalidOperationException("WebSocket text frames are not valid Fitz protocol frames.");
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
        _sendLock.Dispose();
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
