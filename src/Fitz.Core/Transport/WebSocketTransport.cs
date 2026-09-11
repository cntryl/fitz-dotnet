using System.Buffers;
using System.Net.WebSockets;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Transport;

/// <summary>
/// Connects to a Fitz broker over WebSocket, using native PING/PONG for keepalive.
/// </summary>
public sealed class WebSocketTransport : ITransport
{
    readonly Uri _uri;
    readonly TimeSpan _timeout;
    readonly int _maxFrameSize;
    readonly WebSocketOptions? _options;
    readonly HeartbeatOptions _heartbeat;
    readonly SemaphoreSlim _sendLock = new(1, 1);
    ClientWebSocket? _socket;

    /// <summary>Creates a WebSocket transport.</summary>
    public WebSocketTransport(
        Uri url,
        TimeSpan timeout,
        int maxFrameSize,
        WebSocketOptions? options = null,
        HeartbeatOptions? heartbeat = null)
    {
        ArgumentNullException.ThrowIfNull(url);
        if (!url.IsAbsoluteUri || url.Scheme is not ("ws" or "wss"))
        {
            throw new ArgumentException("WebSocket transport requires an absolute ws:// or wss:// URL.", nameof(url));
        }
        if (timeout != Timeout.InfiniteTimeSpan && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be positive or infinite.");
        }
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxFrameSize);

        _uri = url;
        _timeout = timeout;
        _maxFrameSize = maxFrameSize;
        _options = options;
        _heartbeat = heartbeat ?? new HeartbeatOptions();
    }

    /// <inheritdoc />
    public string TransportName => nameof(WebSocketTransport);

    /// <inheritdoc />
    public Uri Url => _uri;

    /// <inheritdoc />
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        if (_socket is { State: WebSocketState.Open })
        {
            return;
        }

        var socket = new ClientWebSocket();
        if (_heartbeat.Enabled)
        {
            ConfigureHeartbeat(socket.Options, _heartbeat);
        }

        if (_options?.Headers is not null)
        {
            foreach (var header in _options.Headers)
            {
                socket.Options.SetRequestHeader(header.Key, header.Value);
            }
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_timeout);
        try
        {
            await socket.ConnectAsync(_uri, timeoutCts.Token).ConfigureAwait(false);
            Interlocked.Exchange(ref _socket, socket)?.Dispose();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            socket.Dispose();
            throw;
        }
        catch (OperationCanceledException exception)
        {
            socket.Dispose();
            throw new TimeoutException($"WebSocket connect exceeded {_timeout}.", exception);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <inheritdoc />
    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
    {
        if (data.Length > _maxFrameSize)
        {
            throw new ProtocolException($"WebSocket frame length exceeds max frame size {_maxFrameSize}.");
        }

        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var socket = EnsureSocket();
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(_timeout);
            try
            {
                await socket.SendAsync(data, WebSocketMessageType.Binary, endOfMessage: true, timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException exception)
            {
                throw new TimeoutException($"WebSocket send exceeded {_timeout}.", exception);
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken ct = default)
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
                        throw new ProtocolException($"WebSocket frame length exceeds max frame size {_maxFrameSize}.");
                    }

                    var nextSize = Math.Min(buffer.Length * 2, _maxFrameSize);
                    var next = ArrayPool<byte>.Shared.Rent(nextSize);
                    buffer.AsSpan(0, length).CopyTo(next);
                    ReturnCleared(buffer, length);
                    buffer = next;
                    remaining = buffer.Length - length;
                }

                var result = await socket.ReceiveAsync(buffer.AsMemory(length, remaining), ct).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    if (ownsBuffer)
                    {
                        ReturnCleared(buffer, length);
                    }

                    return PooledFrame.Closed;
                }
                EnsureBinaryMessage(result.MessageType);

                length += result.Count;
                if (length > _maxFrameSize)
                {
                    throw new ProtocolException($"WebSocket frame length exceeds max frame size {_maxFrameSize}.");
                }

                if (result.EndOfMessage)
                {
                    if (length == 0)
                    {
                        if (ownsBuffer)
                        {
                            ReturnCleared(buffer, length);
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
                ReturnCleared(buffer, length);
            }

            throw;
        }
    }

    /// <summary>
    /// Returns a rented receive buffer after zeroing only the bytes that were filled. The rent is
    /// sized for the largest message we might see, so clearing all of it costs far more than the
    /// frame is worth.
    /// </summary>
    static void ReturnCleared(byte[] buffer, int writtenLength)
    {
        buffer.AsSpan(0, Math.Min(writtenLength, buffer.Length)).Clear();
        ArrayPool<byte>.Shared.Return(buffer);
    }

    internal static void EnsureBinaryMessage(WebSocketMessageType messageType)
    {
        if (messageType == WebSocketMessageType.Text)
        {
            throw new ProtocolException("WebSocket text frames are not valid Fitz protocol frames.");
        }
    }

    internal static void ConfigureHeartbeat(ClientWebSocketOptions options, HeartbeatOptions heartbeat)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(heartbeat);
        options.KeepAliveInterval = heartbeat.Interval ?? TimeSpan.FromSeconds(10);
        options.KeepAliveTimeout = heartbeat.Timeout ?? TimeSpan.FromSeconds(30);
    }

    /// <inheritdoc />
    public async Task CloseAsync(CancellationToken ct = default)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var socket = Interlocked.Exchange(ref _socket, null);

            if (socket is null)
            {
                return;
            }

            try
            {
                if (socket.State == WebSocketState.Open)
                {
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "client closing", ct).ConfigureAwait(false);
                }
            }
            finally
            {
                socket.Dispose();
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>Closes the connection and releases transport resources.</summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        _sendLock.Dispose();
    }

    ClientWebSocket EnsureSocket()
    {
        var socket = Volatile.Read(ref _socket);
        if (socket is null || socket.State != WebSocketState.Open)
        {
            throw new InvalidOperationException("Transport is not connected.");
        }

        return socket;
    }
}
