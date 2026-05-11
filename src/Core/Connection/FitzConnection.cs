using System.IO;
using System.Net.WebSockets;
using System.Text;
using Cntryl.Fitz;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Connection;

public sealed class FitzConnection
{
    private readonly object _gate = new();
    private readonly ClientConfig _config;
    private readonly Func<ITransport> _transportFactory;
    private readonly Multiplexer _multiplexer = new();
    private readonly FrameParser _frameParser;
    private readonly Dictionary<long, Func<CancellationToken, ValueTask>> _reconnectListeners = new();
    private CancellationTokenSource _connectionClosedCts = new();

    private ITransport? _transport;
    private Task? _receiveLoop;
    private CancellationTokenSource? _receiveLoopCts;
    private TaskCompletionSource<bool>? _authFailure;
    private Task? _reconnectTask;
    private bool _closeRequested;
    private long _nextReconnectListenerId;

    public FitzConnection(ClientConfig config, Func<ITransport> transportFactory)
    {
        _config = config;
        _transportFactory = transportFactory;
        _frameParser = new FrameParser(config.MaxFrameSize + FrameCodec.MaxHeaderSize);
    }

    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    internal TimeSpan Timeout => _config.Timeout ?? TimeSpan.FromSeconds(30);
    internal CancellationToken ConnectionClosedToken => _connectionClosedCts.Token;

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _closeRequested = false;

        if (State == ConnectionState.Authenticated)
        {
            return;
        }

        await OpenAndAuthenticateAsync(isReconnect: false, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(ushort messageType, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        EnsureAuthenticated();
        var transport = EnsureTransport();
        var frame = FrameCodec.Encode(messageType, payload.Span);
        var timeout = _config.Timeout ?? TimeSpan.FromSeconds(30);

        try
        {
            var response = await _multiplexer.RequestAsync(
                messageType,
                frame,
                (data, token) => transport.SendAsync(data, token),
                timeout,
                cancellationToken
            ).ConfigureAwait(false);

            return response;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RequestTimeoutException)
        {
            throw;
        }
        catch (ConnectionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            await HandlePossibleTransportFailureAsync(ex).ConfigureAwait(false);
            throw new ConnectionException($"Request failed for message type {messageType}: {ex.Message}");
        }
    }

    public async ValueTask SendAsync(ushort messageType, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        EnsureAuthenticated();
        var frame = FrameCodec.Encode(messageType, payload.Span);

        try
        {
            await EnsureTransport().SendAsync(frame, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await HandlePossibleTransportFailureAsync(ex).ConfigureAwait(false);
            throw;
        }
    }

    public IDisposable RegisterNotificationHandler(ushort messageType, Action<byte[]> handler)
    {
        return _multiplexer.RegisterNotificationHandler(messageType, handler);
    }

    internal IDisposable RegisterBorrowedNotificationHandler(ushort messageType, Action<ReadOnlyMemory<byte>> handler)
    {
        return _multiplexer.RegisterBorrowedNotificationHandler(messageType, handler);
    }

    public IDisposable OnReconnect(Func<CancellationToken, ValueTask> listener)
    {
        ArgumentNullException.ThrowIfNull(listener);

        long listenerId;
        lock (_gate)
        {
            listenerId = ++_nextReconnectListenerId;
            _reconnectListeners[listenerId] = listener;
        }

        return new ReconnectRegistration(this, listenerId);
    }

    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        _closeRequested = true;
        State = ConnectionState.Closed;
        SignalConnectionClosed();
        _authFailure?.TrySetException(new ConnectionException("Connection closed"));
        _multiplexer.SetDisconnected();

        _receiveLoopCts?.Cancel();

        var transport = DetachTransport();
        if (transport is not null)
        {
            try
            {
                await transport.CloseAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
            }
        }

        if (_receiveLoop is not null)
        {
            try
            {
                await _receiveLoop.ConfigureAwait(false);
            }
            catch
            {
            }

            _receiveLoop = null;
        }

        if (transport is not null)
        {
            await transport.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task OpenAndAuthenticateAsync(bool isReconnect, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        State = isReconnect ? ConnectionState.Reconnecting : ConnectionState.Connecting;
        var transport = _transportFactory();
        await transport.ConnectAsync(cancellationToken).ConfigureAwait(false);
        _transport = transport;

        State = ConnectionState.Connected;
        StartReceiveLoop();

        State = ConnectionState.Authenticating;
        _authFailure = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            var tokenProvider = _config.TokenProvider;
            var token = tokenProvider is null ? string.Empty : await tokenProvider(cancellationToken).ConfigureAwait(false);
            var connectFrame = FrameCodec.Encode(MessageTypes.Connect, Encoding.UTF8.GetBytes(token));
            await transport.SendAsync(connectFrame, cancellationToken).ConfigureAwait(false);

            var probeTask = ProbeAuthenticationAsync(transport, cancellationToken);
            var completed = await Task.WhenAny(_authFailure.Task, probeTask).ConfigureAwait(false);
            if (completed == _authFailure.Task)
            {
                await _authFailure.Task.ConfigureAwait(false);
            }

            await probeTask.ConfigureAwait(false);
            RenewConnectionClosedToken();
            State = ConnectionState.Authenticated;
            _multiplexer.SetConnected();

            if (isReconnect)
            {
                await RestoreReconnectStateAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _multiplexer.SetDisconnected();
            State = ConnectionState.Disconnected;

            var detached = DetachTransport();
            if (detached is not null)
            {
                try
                {
                    await detached.CloseAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                }

                await detached.DisposeAsync().ConfigureAwait(false);
            }

            if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
            {
                throw;
            }

            if (_authFailure is not null && (ex is IOException || ex is WebSocketException))
            {
                throw new AuthenticationException(DescribeConnectionLoss(ex));
            }

            throw;
        }
        finally
        {
            _authFailure = null;
        }
    }

    private ITransport EnsureTransport()
    {
        return _transport ?? throw new ConnectionException("No active transport");
    }

    private void EnsureAuthenticated()
    {
        if (_closeRequested || State != ConnectionState.Authenticated)
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
            while (!token.IsCancellationRequested && !_closeRequested)
            {
                try
                {
                    var transport = EnsureTransport();
                    using var data = await transport.ReceiveAsync(token).ConfigureAwait(false);
                    if (data.IsClosed)
                    {
                        throw new ConnectionException("Transport closed.");
                    }

                    if (!data.Memory.IsEmpty)
                    {
                        _frameParser.Append(data.Memory.Span);
                    }
                    while (_frameParser.TryReadFrame(out var frame))
                    {
                        _multiplexer.Dispatch(frame.MessageType, frame.Payload);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested || _closeRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    await HandleConnectionLossAsync(ex).ConfigureAwait(false);
                    return;
                }
            }
        }, token);
    }

    private async Task HandlePossibleTransportFailureAsync(Exception exception)
    {
        if (_closeRequested)
        {
            return;
        }

        await HandleConnectionLossAsync(exception).ConfigureAwait(false);
    }

    private async Task HandleConnectionLossAsync(Exception exception)
    {
        SignalConnectionClosed();
        _multiplexer.SetDisconnected();

        if (State == ConnectionState.Authenticating)
        {
            _authFailure?.TrySetException(new AuthenticationException(DescribeConnectionLoss(exception)));
        }

        if (_closeRequested)
        {
            State = ConnectionState.Closed;
            return;
        }

        State = ConnectionState.Disconnected;

        var transport = DetachTransport();
        if (transport is not null)
        {
            try
            {
                await transport.CloseAsync().ConfigureAwait(false);
            }
            catch
            {
            }

            await transport.DisposeAsync().ConfigureAwait(false);
        }

        var reconnect = _config.Reconnect;
        if (reconnect is null || !reconnect.Enabled)
        {
            return;
        }

        Task reconnectTask;
        lock (_gate)
        {
            _reconnectTask ??= ReconnectLoopAsync();
            reconnectTask = _reconnectTask;
        }

        await reconnectTask.ConfigureAwait(false);
    }

    private async Task ReconnectLoopAsync()
    {
        var reconnect = _config.Reconnect ?? new ReconnectOptions();
        var delay = reconnect.Backoff ?? TimeSpan.FromMilliseconds(250);
        var maxDelay = reconnect.MaxBackoff ?? TimeSpan.FromSeconds(5);
        var attempts = 0;

        try
        {
            while (!_closeRequested && attempts < reconnect.MaxAttempts)
            {
                attempts++;
                State = ConnectionState.Reconnecting;

                try
                {
                    await Task.Delay(delay).ConfigureAwait(false);
                    if (_closeRequested)
                    {
                        return;
                    }

                    await OpenAndAuthenticateAsync(isReconnect: true, CancellationToken.None).ConfigureAwait(false);
                    return;
                }
                catch when (!_closeRequested)
                {
                    var nextDelayMs = Math.Min(delay.TotalMilliseconds * 2, maxDelay.TotalMilliseconds);
                    delay = TimeSpan.FromMilliseconds(nextDelayMs);
                }
            }
        }
        finally
        {
            lock (_gate)
            {
                _reconnectTask = null;
            }
        }
    }

    private async Task RestoreReconnectStateAsync(CancellationToken cancellationToken)
    {
        Func<CancellationToken, ValueTask>[] listeners;
        lock (_gate)
        {
            listeners = _reconnectListeners.Values.ToArray();
        }

        foreach (var listener in listeners)
        {
            await listener(cancellationToken).ConfigureAwait(false);
        }
    }

    private void RenewConnectionClosedToken()
    {
        CancellationTokenSource? previous;
        lock (_gate)
        {
            previous = _connectionClosedCts;
            _connectionClosedCts = new CancellationTokenSource();
        }

        previous.Cancel();
        previous.Dispose();
    }

    private void SignalConnectionClosed()
    {
        CancellationTokenSource current;
        lock (_gate)
        {
            current = _connectionClosedCts;
        }

        if (!current.IsCancellationRequested)
        {
            current.Cancel();
        }
    }

    private void RemoveReconnectListener(long listenerId)
    {
        lock (_gate)
        {
            _reconnectListeners.Remove(listenerId);
        }
    }

    private ITransport? DetachTransport()
    {
        var transport = _transport;
        _transport = null;
        return transport;
    }

    private static string DescribeConnectionLoss(Exception exception)
    {
        return exception is AuthenticationException
            ? exception.Message
            : exception.Message.Length > 0
                ? exception.Message
                : "connection closed during CONNECT";
    }

    private TimeSpan GetDefaultAuthSettleDelay()
    {
        return TimeSpan.FromSeconds(5);
    }

    private async Task ProbeAuthenticationAsync(ITransport transport, CancellationToken cancellationToken)
    {
        var probeTimeout = _config.AuthSettleDelay is { } configuredTimeout && configuredTimeout > TimeSpan.Zero
            ? configuredTimeout
            : GetDefaultAuthSettleDelay();

        try
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteString("lease://fitz/system/auth-probe");
            var response = await _multiplexer.RequestAsync(
                MessageTypes.LeaseQuery,
                FrameCodec.Encode(MessageTypes.LeaseQuery, writer.WrittenSpan),
                (data, token) => transport.SendAsync(data, token),
                probeTimeout,
                cancellationToken).ConfigureAwait(false);

            if (response.Length == 0 || response[0] != 0)
            {
                throw new AuthenticationException("CONNECT verification failed");
            }
        }
        catch (RequestTimeoutException)
        {
            throw new AuthenticationException("CONNECT verification timed out");
        }
        catch (ConnectionException ex)
        {
            throw new AuthenticationException(DescribeConnectionLoss(ex));
        }
    }

    private sealed class ReconnectRegistration : IDisposable
    {
        private readonly FitzConnection _owner;
        private readonly long _listenerId;
        private int _disposed;

        internal ReconnectRegistration(FitzConnection owner, long listenerId)
        {
            _owner = owner;
            _listenerId = listenerId;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _owner.RemoveReconnectListener(_listenerId);
        }
    }
}
