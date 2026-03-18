using System.Text;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Connection;

public sealed class FitzConnection
{
    private const int ReceiveBufferSize = 64 * 1024;
    private readonly ClientConfig _config;
    private readonly Func<ITransport> _transportFactory;
    private readonly Multiplexer _multiplexer = new();
    private ITransport? _transport;
    private Task? _receiveLoop;
    private CancellationTokenSource? _receiveLoopCts;

    public FitzConnection(ClientConfig config, Func<ITransport> transportFactory)
    {
        _config = config;
        _transportFactory = transportFactory;
    }

    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (State == ConnectionState.Authenticated)
        {
            return;
        }

        State = ConnectionState.Connecting;
        _transport = _transportFactory();
        await _transport.ConnectAsync(cancellationToken);
        StartReceiveLoop();

        State = ConnectionState.Authenticating;
        var tokenProvider = _config.TokenProvider;
        var token = tokenProvider is null ? string.Empty : await tokenProvider(cancellationToken);

        var connectFrame = FrameCodec.Encode(MessageTypes.Connect, Encoding.UTF8.GetBytes(token));
        await _transport.SendAsync(connectFrame, cancellationToken);

        var settleDelay = _config.AuthSettleDelay ?? TimeSpan.FromMilliseconds(100);
        if (settleDelay > TimeSpan.Zero)
        {
            await Task.Delay(settleDelay, cancellationToken);
        }

        State = ConnectionState.Authenticated;
        _multiplexer.SetConnected();
    }

    public async Task<byte[]> RequestAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default)
    {
        EnsureAuthenticated();
        var transport = EnsureTransport();
        var frame = FrameCodec.Encode(messageType, payload);
        var timeout = _config.Timeout ?? TimeSpan.FromSeconds(30);

        try
        {
            return await _multiplexer.RequestAsync(
                messageType,
                frame,
                (data, token) => transport.SendAsync(data, token),
                timeout,
                cancellationToken
            );
        }
        catch (RequestTimeoutException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new ConnectionException($"Request failed for message type {messageType}: {ex.Message}");
        }
    }

    public async Task SendAsync(ushort messageType, byte[] payload, CancellationToken cancellationToken = default)
    {
        EnsureAuthenticated();
        var frame = FrameCodec.Encode(messageType, payload);
        await EnsureTransport().SendAsync(frame, cancellationToken);
    }

    public void RegisterNotificationHandler(ushort messageType, Action<byte[]> handler)
    {
        _multiplexer.RegisterNotificationHandler(messageType, handler);
    }

    public void UnregisterNotificationHandler(ushort messageType)
    {
        _multiplexer.UnregisterNotificationHandler(messageType);
    }

    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        _receiveLoopCts?.Cancel();
        if (_receiveLoop is not null)
        {
            await _receiveLoop;
            _receiveLoop = null;
        }

        var transport = _transport;
        _transport = null;

        if (transport is not null)
        {
            await transport.CloseAsync(cancellationToken);
            await transport.DisposeAsync();
        }

        _multiplexer.SetDisconnected();
        State = ConnectionState.Closed;
    }

    private ITransport EnsureTransport()
    {
        return _transport ?? throw new ConnectionException("No active transport");
    }

    private void EnsureAuthenticated()
    {
        if (State != ConnectionState.Authenticated)
        {
            throw new ConnectionException($"Cannot use connection while state is {State}");
        }
    }

    private void StartReceiveLoop()
    {
        _receiveLoopCts?.Cancel();
        _receiveLoopCts = new CancellationTokenSource();
        var token = _receiveLoopCts.Token;

        _receiveLoop = Task.Run(async () =>
        {
            var buffer = new byte[ReceiveBufferSize];
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var received = await EnsureTransport().ReceiveAsync(buffer, token);
                    if (received <= 0)
                    {
                        await Task.Delay(25, token);
                        continue;
                    }

                    var frame = FrameCodec.Decode(buffer.AsSpan(0, received));
                    _multiplexer.Dispatch(frame.MessageType, frame.Payload);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception)
                {
                    _multiplexer.SetDisconnected();
                    if (State != ConnectionState.Closed)
                    {
                        State = ConnectionState.Disconnected;
                    }

                    return;
                }
            }
        }, token);
    }
}