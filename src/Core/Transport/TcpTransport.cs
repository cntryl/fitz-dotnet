using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace Cntryl.Fitz.Transport;

public sealed class TcpTransport : ITransport
{
    private readonly Uri _uri;
    private readonly TimeSpan _timeout;
    private readonly int _maxFrameSize;
    private readonly HeartbeatOptions _heartbeat;
    private readonly byte[] _receiveHeaderBuffer = new byte[4];
    private readonly byte[] _sendHeaderBuffer = new byte[4];
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private TcpClient? _client;
    private NetworkStream? _stream;

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

        await client.ConnectAsync(_uri.Host, _uri.Port, timeoutCts.Token).ConfigureAwait(false);
        if (_heartbeat.Enabled)
        {
            client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        }

        _client = client;
        _stream = client.GetStream();
    }

    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        var stream = EnsureStream();
        var frameLength = data.Length;
        if (frameLength > _maxFrameSize)
        {
            throw new InvalidOperationException($"TCP frame length {frameLength} exceeds max frame size {_maxFrameSize}.");
        }

        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            BinaryPrimitives.WriteUInt32BigEndian(_sendHeaderBuffer, (uint)frameLength);

            using var timeoutCts = new CancellationTokenSource(_timeout);
            using var cancellationRegistration = cancellationToken.CanBeCanceled
                ? cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), timeoutCts)
                : default;

            await stream.WriteAsync(_sendHeaderBuffer.AsMemory(0, 4), timeoutCts.Token).ConfigureAwait(false);
            if (!data.IsEmpty)
            {
                await stream.WriteAsync(data, timeoutCts.Token).ConfigureAwait(false);
            }
        }
        finally
        {
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

        var frameLength = checked((int)BinaryPrimitives.ReadUInt32BigEndian(_receiveHeaderBuffer));
        if (frameLength == 0)
        {
            return PooledFrame.Empty;
        }

        if (frameLength > _maxFrameSize)
        {
            throw new InvalidOperationException($"TCP frame length {frameLength} exceeds max frame size {_maxFrameSize}.");
        }

        var payload = ArrayPool<byte>.Shared.Rent(frameLength);
        var payloadRead = await ReadExactOrClosedAsync(stream, payload.AsMemory(0, frameLength), cancellationToken).ConfigureAwait(false);
        if (payloadRead == 0)
        {
            ArrayPool<byte>.Shared.Return(payload);
            return PooledFrame.Closed;
        }

        return PooledFrame.FromRentedBuffer(payload, frameLength);
    }

    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        var stream = _stream;
        _stream = null;

        var client = _client;
        _client = null;

        if (stream is not null)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }

        client?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        _sendLock.Dispose();
    }

    private static async Task<int> ReadExactOrClosedAsync(NetworkStream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var bytesRead = await stream.ReadAsync(buffer[totalRead..], cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                return 0;
            }

            totalRead += bytesRead;
        }

        return totalRead;
    }

    private NetworkStream EnsureStream()
    {
        if (_stream is null || _client is null || !_client.Connected)
        {
            throw new InvalidOperationException("Transport is not connected.");
        }

        return _stream;
    }
}
