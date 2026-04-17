using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace Cntryl.Fitz.Transport;

public sealed class TcpTransport : ITransport
{
    private readonly Uri _uri;
    private readonly TimeSpan _timeout;
    private readonly int _maxFrameSize;
    private readonly byte[] _receiveHeaderBuffer = new byte[4];
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private TcpClient? _client;
    private NetworkStream? _stream;

    public TcpTransport(string url, TimeSpan timeout, int maxFrameSize)
    {
        _uri = CreateUri(url);
        _timeout = timeout;
        _maxFrameSize = maxFrameSize;
    }

    public string Url => _uri.ToString();

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

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_timeout);

        await client.ConnectAsync(_uri.Host, _uri.Port, cts.Token).ConfigureAwait(false);

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

        var totalLength = 4 + frameLength;
        var buffer = ArrayPool<byte>.Shared.Rent(totalLength);
        try
        {
            BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(0, 4), (uint)frameLength);
            if (!data.IsEmpty)
            {
                data.Span.CopyTo(buffer.AsSpan(4));
            }

            await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(_timeout);
                await stream.WriteAsync(buffer.AsMemory(0, totalLength), cts.Token).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async ValueTask<PooledFrame> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var stream = EnsureStream();

        var headerRead = await ReadExactOrClosedAsync(stream, _receiveHeaderBuffer, cancellationToken).ConfigureAwait(false);
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
        var payloadRead = await ReadExactOrClosedAsync(stream, payload, cancellationToken).ConfigureAwait(false);
        if (payloadRead == 0)
        {
            ArrayPool<byte>.Shared.Return(payload);
            return PooledFrame.Closed;
        }

        return PooledFrame.FromRentedBuffer(payload, frameLength);
    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        var stream = _stream;
        _stream = null;

        var client = _client;
        _client = null;

        stream?.Dispose();
        client?.Dispose();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
    }

    private static async Task<int> ReadExactOrClosedAsync(NetworkStream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var bytesRead = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                return 0;
            }

            totalRead += bytesRead;
        }

        return totalRead;
    }

    private static Uri CreateUri(string url)
    {
        if (url.StartsWith("tcp://", StringComparison.OrdinalIgnoreCase))
        {
            return new Uri(url);
        }

        return new Uri($"tcp://{url}");
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
