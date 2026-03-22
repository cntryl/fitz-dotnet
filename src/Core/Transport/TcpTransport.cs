using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;

namespace Cntryl.Fitz.Transport;

public sealed class TcpTransport : ITransport
{
    private readonly Uri _uri;
    private readonly TimeSpan _timeout;
    private readonly int _maxFrameSize;
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

        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_timeout);

            var header = ArrayPool<byte>.Shared.Rent(4);
            try
            {
                BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), (uint)frameLength);
                await stream.WriteAsync(header.AsMemory(0, 4), cts.Token).ConfigureAwait(false);
                if (!data.IsEmpty)
                {
                    await stream.WriteAsync(data, cts.Token).ConfigureAwait(false);
                }

                await stream.FlushAsync(cts.Token).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(header);
            }
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async Task<byte[]> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        var stream = EnsureStream();

        var header = new byte[4];
        var headerRead = await ReadExactOrClosedAsync(stream, header, cancellationToken).ConfigureAwait(false);
        if (headerRead == 0)
        {
            return [];
        }

        var frameLength = checked((int)BinaryPrimitives.ReadUInt32BigEndian(header));
        if (frameLength == 0)
        {
            return [];
        }

        if (frameLength > _maxFrameSize)
        {
            throw new InvalidOperationException($"TCP frame length {frameLength} exceeds max frame size {_maxFrameSize}.");
        }

        var payload = GC.AllocateUninitializedArray<byte>(frameLength);
        var payloadRead = await ReadExactOrClosedAsync(stream, payload, cancellationToken).ConfigureAwait(false);
        if (payloadRead == 0)
        {
            return [];
        }

        return payload;
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
