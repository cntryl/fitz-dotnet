using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;
using Cntryl.Fitz.Errors;

namespace Cntryl.Fitz.Transport;

public sealed class TcpTransport : ITransport
{
    readonly Uri _uri;
    readonly TimeSpan _timeout;
    readonly int _maxFrameSize;
    readonly HeartbeatOptions _heartbeat;
    readonly byte[] _receiveHeaderBuffer = new byte[4];
    readonly SemaphoreSlim _sendLock = new(1, 1);
    TcpClient? _client;
    NetworkStream? _stream;

    public TcpTransport(Uri url, TimeSpan timeout, int maxFrameSize, HeartbeatOptions? heartbeat = null)
    {
        ArgumentNullException.ThrowIfNull(url);
        if (!url.IsAbsoluteUri || !url.Scheme.Equals("tcp", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("TCP transport requires an absolute tcp:// URL.", nameof(url));
        }

        _uri = url;
        _timeout = timeout;
        _maxFrameSize = maxFrameSize;
        _heartbeat = heartbeat ?? new HeartbeatOptions();
    }

    public Uri Url => _uri;

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to the transport only after a successful connection; every failure path disposes the local client.")]
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_client is { Connected: true })
        {
            return;
        }

        var client = new TcpClient
        {
            NoDelay = true,
        };

        using var timeoutCts = new CancellationTokenSource(_timeout);
        using var cancellationRegistration = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), timeoutCts)
            : default;

        try
        {
            await client.ConnectAsync(_uri.Host, _uri.Port, timeoutCts.Token).ConfigureAwait(false);
            ConfigureKeepAlive(client.Client);

            var previousClient = Interlocked.Exchange(ref _client, client);
            var previousStream = Interlocked.Exchange(ref _stream, client.GetStream());
            if (previousStream is not null)
            {
                await previousStream.DisposeAsync().ConfigureAwait(false);
            }
            previousClient?.Dispose();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            client.Dispose();
            throw;
        }
        catch (OperationCanceledException exception)
        {
            client.Dispose();
            throw new TimeoutException($"TCP connect exceeded {_timeout}.", exception);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var frameLength = data.Length;
        if (frameLength > _maxFrameSize)
        {
            throw new ProtocolException($"TCP frame length {frameLength} exceeds max frame size {_maxFrameSize}.");
        }

        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        byte[]? frame = null;
        try
        {
            var stream = EnsureStream();
            frame = ArrayPool<byte>.Shared.Rent(checked(frameLength + 4));
            BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(0, 4), (uint)frameLength);
            data.Span.CopyTo(frame.AsSpan(4, frameLength));

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_timeout);
            try
            {
                await stream.WriteAsync(frame.AsMemory(0, frameLength + 4), timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException exception)
            {
                throw new TimeoutException($"TCP send exceeded {_timeout}.", exception);
            }
        }
        finally
        {
            if (frame is not null)
            {
                ArrayPool<byte>.Shared.Return(frame, clearArray: true);
            }
            _sendLock.Release();
        }
    }

    public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var stream = EnsureStream();

        var headerRead = await ReadExactOrClosedAsync(stream, _receiveHeaderBuffer.AsMemory(), cancellationToken).ConfigureAwait(false);
        if (headerRead == 0)
        {
            return PooledFrame.Closed;
        }

        var encodedFrameLength = BinaryPrimitives.ReadUInt32BigEndian(_receiveHeaderBuffer);
        if (encodedFrameLength > int.MaxValue)
        {
            throw new ProtocolException($"TCP frame length {encodedFrameLength} cannot be represented by this client.");
        }

        var frameLength = (int)encodedFrameLength;
        if (frameLength == 0)
        {
            return PooledFrame.Empty;
        }

        if (frameLength > _maxFrameSize)
        {
            throw new ProtocolException($"TCP frame length {frameLength} exceeds max frame size {_maxFrameSize}.");
        }

        var payload = ArrayPool<byte>.Shared.Rent(frameLength);
        var ownsPayload = true;
        try
        {
            var payloadRead = await ReadExactOrClosedAsync(stream, payload.AsMemory(0, frameLength), cancellationToken).ConfigureAwait(false);
            if (payloadRead == 0)
            {
                return PooledFrame.Closed;
            }

            var frame = PooledFrame.FromRentedBuffer(payload, frameLength);
            ownsPayload = false;
            return frame;
        }
        finally
        {
            if (ownsPayload)
            {
                ArrayPool<byte>.Shared.Return(payload, clearArray: true);
            }
        }
    }

    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = Interlocked.Exchange(ref _stream, null);
            var client = Interlocked.Exchange(ref _client, null);

            if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            client?.Dispose();
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        _sendLock.Dispose();
    }

    static async Task<int> ReadExactOrClosedAsync(NetworkStream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var bytesRead = await stream.ReadAsync(buffer[totalRead..], cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                if (totalRead > 0)
                {
                    throw new EndOfStreamException("Connection closed in the middle of a frame.");
                }

                return 0;
            }

            totalRead += bytesRead;
        }

        return totalRead;
    }

    NetworkStream EnsureStream()
    {
        var stream = Volatile.Read(ref _stream);
        var client = Volatile.Read(ref _client);
        if (stream is null || client is null || !client.Connected)
        {
            throw new InvalidOperationException("Transport is not connected.");
        }

        return stream;
    }

    void ConfigureKeepAlive(Socket socket)
    {
        if (!_heartbeat.Enabled)
        {
            return;
        }

        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        var interval = _heartbeat.Interval ?? TimeSpan.FromSeconds(10);
        var timeout = _heartbeat.Timeout ?? TimeSpan.FromSeconds(30);
        try
        {
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime,
                Math.Max(1, checked((int)Math.Ceiling(interval.TotalSeconds))));
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval,
                Math.Max(1, checked((int)Math.Ceiling(interval.TotalSeconds))));
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount,
                Math.Max(1, checked((int)Math.Ceiling(timeout.TotalSeconds / interval.TotalSeconds))));
        }
        catch (PlatformNotSupportedException)
        {
            // SO_KEEPALIVE remains enabled when detailed tuning is unavailable.
        }
        catch (SocketException)
        {
            // SO_KEEPALIVE remains enabled when the platform rejects detailed tuning.
        }
    }
}
