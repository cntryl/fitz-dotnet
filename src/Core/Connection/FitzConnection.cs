using System.IO;
using System.Net.WebSockets;
using System.Text;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Connection;

public sealed class FitzConnection
{
    private static readonly TaskCompletionSource<bool> CompletedStateSignal = CreateCompletedStateSignal();

    private readonly object _gate = new();
    private readonly ClientConfig _config;
    private readonly Func<ITransport> _transportFactory;
    private readonly Multiplexer _multiplexer = new();
    private readonly FrameParser _frameParser;
    private readonly Dictionary<long, Func<CancellationToken, ValueTask>> _reconnectListeners = new();
    private readonly Dictionary<long, Action> _disconnectListeners = new();
    private readonly AsyncHandlerDispatcher _asyncHandlerDispatcher;
    private CancellationTokenSource _connectionClosedCts = new();
    private RequestGate _requestGate;
    private TaskCompletionSource<bool> _stateChanged = CompletedStateSignal;

    private ITransport? _transport;
    private Task? _receiveLoop;
    private CancellationTokenSource? _receiveLoopCts;
    private TaskCompletionSource<bool>? _authFailure;
    private Task? _connectTask;
    private Task? _reconnectTask;
    private Timer? _heartbeatTimer;
    private bool _authAttemptIsReconnect;
    private bool _closeRequested;
    private bool _authRejected;
    private bool _sessionConfirmed;
    private bool _hasEstablishedSession;
    private bool _reconnectExhausted;
    private long _nextReconnectListenerId;
    private long _nextDisconnectListenerId;
    private long _readyWaiterCount;
    private DateTimeOffset _lastInboundActivity = DateTimeOffset.UtcNow;
    private DateTimeOffset _lastOutboundActivity = DateTimeOffset.UtcNow;
    private ConnectionState _state = ConnectionState.Disconnected;

    public FitzConnection(ClientConfig config, Func<ITransport> transportFactory)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _transportFactory = transportFactory ?? throw new ArgumentNullException(nameof(transportFactory));
        _frameParser = new FrameParser(config.MaxFrameSize + FrameCodec.MaxHeaderSize);
        _requestGate = CreateRequestGate();
        _asyncHandlerDispatcher = new AsyncHandlerDispatcher(
            _config.ResolvedAsyncHandlers.MaxConcurrency,
            ResolveAsyncHandlerTimeout(),
            Math.Min(Math.Max(_config.ResolvedMaxRequestQueueSize, 0), 1024),
            OnAsyncHandlerError,
            onMetricsChanged: OnAsyncHandlerMetricsChanged,
            onSaturated: OnAsyncHandlerSaturated);
    }

    public ConnectionState State => _state;

    internal TimeSpan Timeout => _config.Timeout ?? TimeSpan.FromSeconds(30);
    internal CancellationToken ConnectionClosedToken => _connectionClosedCts.Token;

    internal bool TryDispatchAsyncHandler(Func<CancellationToken, ValueTask> handler)
    {
        return _asyncHandlerDispatcher.TryDispatch(handler);
    }

    internal async ValueTask<T> ExecuteWithRetryAsync<T>(
        RetryOperation operation,
        Func<CancellationToken, ValueTask<T>> task,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(task);

        var retry = _config.ResolvedRetry;
        if (!retry.Enabled || operation.RetryClass == RetryClass.WaitOnly)
        {
            return await task(cancellationToken).ConfigureAwait(false);
        }

        var attempts = 0;
        var delay = retry.Backoff ?? TimeSpan.FromMilliseconds(100);
        var maxDelay = retry.MaxBackoff ?? TimeSpan.FromSeconds(1);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempts++;

            try
            {
                return await task(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (ShouldRetry(operation, ex) && attempts < retry.MaxAttempts)
            {
                RecordRetry(operation, attempts, delay, ex);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = Min(TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2), maxDelay);
            }
            catch (Exception ex) when (ShouldRetry(operation, ex))
            {
                RecordRetryExhausted(operation, attempts, ex);
                throw;
            }
        }
    }

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_closeRequested || State == ConnectionState.Closed)
        {
            throw new ConnectionException("Connection closed");
        }

        if (State == ConnectionState.Authenticated)
        {
            return;
        }

        Task? connectTask;
        bool waitForReady;

        lock (_gate)
        {
            connectTask = _connectTask;
            waitForReady = connectTask is null
                && (State is ConnectionState.Connecting
                    or ConnectionState.Connected
                    or ConnectionState.Authenticating
                    or ConnectionState.Reconnecting
                    || (State == ConnectionState.Disconnected && CanWaitForReconnectUnsafe()));

            if (connectTask is null && !waitForReady)
            {
                _closeRequested = false;
                _authRejected = false;
                _reconnectExhausted = false;
                connectTask = TrackConnectTask(() => OpenAndAuthenticateAsync(isReconnect: false, cancellationToken));
            }
        }

        if (connectTask is not null)
        {
            await connectTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await WaitUntilReadyAsync(Timeout, cancellationToken).ConfigureAwait(false);
    }

    internal async Task WaitUntilReadyAsync(TimeSpan waitTimeout, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var releaseWaiter = TryAcquireReadyWaitSlot();
        var deadline = waitTimeout == System.Threading.Timeout.InfiniteTimeSpan
            ? DateTimeOffset.MaxValue
            : DateTimeOffset.UtcNow + waitTimeout;

        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var failure = GetReadyFailure();
                if (State == ConnectionState.Authenticated)
                {
                    return;
                }

                if (failure is not null)
                {
                    throw failure;
                }

                Task waitTask;
                lock (_gate)
                {
                    waitTask = _stateChanged.Task;
                }

                if (waitTimeout == System.Threading.Timeout.InfiniteTimeSpan)
                {
                    await waitTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var remaining = deadline - DateTimeOffset.UtcNow;
                if (remaining <= TimeSpan.Zero)
                {
                    throw new ConnectionException("Timed out waiting for connection to become ready");
                }

                await waitTask.WaitAsync(remaining, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            releaseWaiter?.Dispose();
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        await WaitForRequestReadyAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            using var slot = await AcquireRequestSlotAsync(cancellationToken).ConfigureAwait(false);
            var transport = EnsureTransport();
            var frame = FrameCodec.Encode(messageType, payload.Span);
            MarkOutboundActivity();

            var response = await _multiplexer.RequestAsync(
                messageType,
                frame,
                (data, token) => transport.SendAsync(data, token),
                Timeout,
                cancellationToken).ConfigureAwait(false);

            return response;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RequestTimeoutException)
        {
            RecordRequestTimeout(messageType);
            throw;
        }
        catch (RequestQueueFullException)
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
            throw new ConnectionException($"Request failed for message type {messageType}: {ex.Message}", ex);
        }
    }

    public async ValueTask SendAsync(
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken = default)
    {
        await WaitForRequestReadyAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            using var slot = await AcquireRequestSlotAsync(cancellationToken).ConfigureAwait(false);
            var frame = FrameCodec.Encode(messageType, payload.Span);
            MarkOutboundActivity();
            await EnsureTransport().SendAsync(frame, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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

    public IDisposable OnDisconnect(Action listener)
    {
        ArgumentNullException.ThrowIfNull(listener);

        long listenerId;
        lock (_gate)
        {
            listenerId = ++_nextDisconnectListenerId;
            _disconnectListeners[listenerId] = listener;
        }

        return new DisconnectRegistration(this, listenerId);
    }

    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        _closeRequested = true;
        StopHeartbeat();
        _asyncHandlerDispatcher.Close();
        SetState(ConnectionState.Closed);
        NotifyDisconnect();
        SignalConnectionClosed();
        _authFailure?.TrySetException(new ConnectionException("Connection closed"));
        _requestGate.Close();
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

        await _asyncHandlerDispatcher.DrainAsync().ConfigureAwait(false);
        EmitLifecycleEvent("closed");
    }

    private Task TrackConnectTask(Func<Task> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _connectTask = completion.Task;
        _ = RunAsync(completion, operation);
        return completion.Task;

        async Task RunAsync(TaskCompletionSource<bool> trackedTask, Func<Task> inner)
        {
            try
            {
                await inner().ConfigureAwait(false);
                trackedTask.TrySetResult(true);
            }
            catch (Exception ex)
            {
                trackedTask.TrySetException(ex);
            }
            finally
            {
                lock (_gate)
                {
                    if (ReferenceEquals(_connectTask, trackedTask.Task))
                    {
                        _connectTask = null;
                    }
                }
            }
        }
    }

    private async Task WaitForRequestReadyAsync(CancellationToken cancellationToken)
    {
        if (State == ConnectionState.Authenticated)
        {
            return;
        }

        await WaitUntilReadyAsync(Timeout, cancellationToken).ConfigureAwait(false);
        EnsureAuthenticated();
    }

    private async Task OpenAndAuthenticateAsync(bool isReconnect, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        ResetSessionState(isReconnect);
        _requestGate = CreateRequestGate();
        StopHeartbeat();

        SetState(isReconnect ? ConnectionState.Reconnecting : ConnectionState.Connecting);
        EmitLifecycleEvent(isReconnect ? "reconnect_start" : "connect_start");

        var transport = _transportFactory();
        _transport = transport;

        try
        {
            await transport.ConnectAsync(cancellationToken).ConfigureAwait(false);
            MarkInboundActivity();

            SetState(ConnectionState.Connected);
            _multiplexer.BeginSession();
            StartReceiveLoop();

            SetState(ConnectionState.Authenticating);
            _authAttemptIsReconnect = isReconnect;
            _authFailure = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var tokenProvider = _config.TokenProvider;
            var token = tokenProvider is null
                ? string.Empty
                : await tokenProvider(cancellationToken).ConfigureAwait(false);
            var connectFrame = FrameCodec.Encode(MessageTypes.Connect, Encoding.UTF8.GetBytes(token));
            await transport.SendAsync(connectFrame, cancellationToken).ConfigureAwait(false);
            MarkOutboundActivity();

            await WaitForAuthSettlementAsync(cancellationToken).ConfigureAwait(false);

            RenewConnectionClosedToken();
            SetState(ConnectionState.Authenticated);
            _multiplexer.SetConnected();
            _reconnectExhausted = false;
            StartHeartbeat();

            if (isReconnect)
            {
                await RestoreReconnectStateAsync(cancellationToken).ConfigureAwait(false);
            }

            EmitLifecycleEvent(isReconnect ? "reconnect_succeeded" : "connect_succeeded");
        }
        catch (Exception ex)
        {
            StopHeartbeat();
            _requestGate.Close();
            _multiplexer.SetDisconnected();

            var authFailure = ex is AuthenticationException
                ? ex
                : !isReconnect && IsUnconfirmedSessionLoss()
                    ? new AuthenticationException(DescribeConnectionLoss(ex), ex)
                    : ex;

            if (authFailure is AuthenticationException)
            {
                _authRejected = true;
                SetState(ConnectionState.Closed);
                EmitLifecycleEvent("auth_rejected", authFailure);
            }
            else if (_closeRequested)
            {
                SetState(ConnectionState.Closed);
            }
            else
            {
                SetState(ConnectionState.Disconnected);
                EmitLifecycleEvent(isReconnect ? "reconnect_failed" : "connect_failed", authFailure);
            }

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

            throw authFailure;
        }
        finally
        {
            _authFailure = null;
            _authAttemptIsReconnect = false;
        }
    }

    private async Task WaitForAuthSettlementAsync(CancellationToken cancellationToken)
    {
        var settleDelay = _config.AuthSettleDelay is { } configured && configured >= TimeSpan.Zero
            ? configured
            : GetDefaultAuthSettleDelay();

        if (_authFailure is null)
        {
            return;
        }

        if (settleDelay == TimeSpan.Zero)
        {
            if (_authFailure.Task.IsCompleted)
            {
                await _authFailure.Task.ConfigureAwait(false);
            }

            return;
        }

        var delayTask = Task.Delay(settleDelay, cancellationToken);
        var completed = await Task.WhenAny(_authFailure.Task, delayTask).ConfigureAwait(false);
        if (completed == _authFailure.Task)
        {
            await _authFailure.Task.ConfigureAwait(false);
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
                    MarkInboundActivity();
                    if (data.IsClosed)
                    {
                        throw new ConnectionException("Transport closed.");
                    }

                    if (!data.Memory.IsEmpty)
                    {
                        _frameParser.Append(data.Memory.Span);
                    }

                    var observedFrame = false;
                    while (_frameParser.TryReadFrame(out var frame))
                    {
                        observedFrame = true;
                        _multiplexer.Dispatch(frame.MessageType, frame.Payload);
                    }

                    if (observedFrame)
                    {
                        ConfirmSession();
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

        if (State is not ConnectionState.Authenticated and not ConnectionState.Authenticating and not ConnectionState.Reconnecting)
        {
            return;
        }

        await HandleConnectionLossAsync(exception).ConfigureAwait(false);
    }

    private async Task HandleConnectionLossAsync(Exception exception)
    {
        StopHeartbeat();
        SignalConnectionClosed();
        NotifyDisconnect();
        _requestGate.Close();
        _multiplexer.SetDisconnected();

        if (!_authAttemptIsReconnect && (State == ConnectionState.Authenticating || IsUnconfirmedSessionLoss()))
        {
            _authRejected = true;
            var authError = exception as AuthenticationException
                ?? new AuthenticationException(DescribeConnectionLoss(exception), exception);
            _authFailure?.TrySetException(authError);
        }

        if (_closeRequested)
        {
            SetState(ConnectionState.Closed);
            return;
        }

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

        if (_authRejected)
        {
            SetState(ConnectionState.Closed);
            EmitLifecycleEvent("auth_rejected", exception);
            return;
        }

        SetState(ConnectionState.Disconnected);
        EmitLifecycleEvent("connection_lost", exception);

        var reconnect = _config.ResolvedReconnect;
        if (!reconnect.Enabled || !_hasEstablishedSession)
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
        var reconnect = _config.ResolvedReconnect;
        var delay = reconnect.Backoff ?? TimeSpan.FromMilliseconds(250);
        var maxDelay = reconnect.MaxBackoff ?? TimeSpan.FromSeconds(5);
        var attempts = 0;

        try
        {
            while (!_closeRequested && attempts < reconnect.MaxAttempts && !_authRejected)
            {
                attempts++;
                SetState(ConnectionState.Reconnecting);

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
                catch when (!_closeRequested && !_authRejected)
                {
                    delay = Min(TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2), maxDelay);
                }
            }

            _reconnectExhausted = true;
            if (!_closeRequested && !_authRejected)
            {
                SetState(ConnectionState.Disconnected);
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

    private void ResetSessionState(bool isReconnect)
    {
        _sessionConfirmed = false;
        _authRejected = false;
        if (!isReconnect)
        {
            _hasEstablishedSession = false;
        }
    }

    private void ConfirmSession()
    {
        _sessionConfirmed = true;
        _hasEstablishedSession = true;
    }

    private bool IsUnconfirmedSessionLoss()
    {
        return !_sessionConfirmed
            && State is ConnectionState.Authenticating or ConnectionState.Authenticated;
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

    private void NotifyDisconnect()
    {
        Action[] listeners;
        lock (_gate)
        {
            listeners = _disconnectListeners.Values.ToArray();
        }

        foreach (var listener in listeners)
        {
            try
            {
                listener();
            }
            catch
            {
            }
        }
    }

    private void RemoveReconnectListener(long listenerId)
    {
        lock (_gate)
        {
            _reconnectListeners.Remove(listenerId);
        }
    }

    private void RemoveDisconnectListener(long listenerId)
    {
        lock (_gate)
        {
            _disconnectListeners.Remove(listenerId);
        }
    }

    private ITransport? DetachTransport()
    {
        var transport = _transport;
        _transport = null;
        return transport;
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

    private RequestGate CreateRequestGate()
    {
        return new RequestGate(Math.Max(1, _config.MaxInFlightRequests), _config.ResolvedMaxRequestQueueSize);
    }

    private async Task<RequestGate.Releaser> AcquireRequestSlotAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _requestGate.AcquireAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RequestQueueFullException)
        {
            RecordRequestQueueFull();
            throw;
        }
    }

    private IDisposable? TryAcquireReadyWaitSlot()
    {
        if (State == ConnectionState.Authenticated || GetReadyFailure() is not null)
        {
            return null;
        }

        if (_config.ResolvedMaxRequestQueueSize <= 0)
        {
            throw new RequestQueueFullException("The Fitz connection wait queue is full.");
        }

        var current = Interlocked.Increment(ref _readyWaiterCount);
        if (current > _config.ResolvedMaxRequestQueueSize)
        {
            Interlocked.Decrement(ref _readyWaiterCount);
            throw new RequestQueueFullException("The Fitz connection wait queue is full.");
        }

        return new ReadyWaitRegistration(this);
    }

    private Exception? GetReadyFailure()
    {
        if (State == ConnectionState.Authenticated)
        {
            return null;
        }

        if (_authRejected)
        {
            return new AuthenticationException("Authentication rejected");
        }

        if (_closeRequested || State == ConnectionState.Closed)
        {
            return new ConnectionException("Connection closed");
        }

        if (State is ConnectionState.Connecting
            or ConnectionState.Connected
            or ConnectionState.Authenticating
            or ConnectionState.Reconnecting)
        {
            return null;
        }

        if (State == ConnectionState.Disconnected && CanWaitForReconnect())
        {
            return null;
        }

        return new ConnectionException($"Cannot use connection while state is {State}");
    }

    private bool CanWaitForReconnect()
    {
        lock (_gate)
        {
            return CanWaitForReconnectUnsafe();
        }
    }

    private bool CanWaitForReconnectUnsafe()
    {
        var reconnect = _config.ResolvedReconnect;
        return reconnect.Enabled
            && _hasEstablishedSession
            && !_reconnectExhausted
            && !_authRejected
            && !_closeRequested;
    }

    private void SetState(ConnectionState next)
    {
        TaskCompletionSource<bool> previous;
        lock (_gate)
        {
            _state = next;
            previous = _stateChanged;
            _stateChanged = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        previous.TrySetResult(true);
    }

    private void StartHeartbeat()
    {
        StopHeartbeat();

        var heartbeat = _config.ResolvedHeartbeat;
        if (!heartbeat.Enabled)
        {
            return;
        }

        var interval = heartbeat.Interval ?? TimeSpan.FromSeconds(10);
        var timeout = heartbeat.Timeout ?? TimeSpan.FromSeconds(30);
        _lastInboundActivity = DateTimeOffset.UtcNow;
        _lastOutboundActivity = _lastInboundActivity;

        _heartbeatTimer = new Timer(_ =>
        {
            if (_closeRequested || State != ConnectionState.Authenticated)
            {
                return;
            }

            var now = DateTimeOffset.UtcNow;
            var lastActivity = _lastInboundActivity >= _lastOutboundActivity
                ? _lastInboundActivity
                : _lastOutboundActivity;

            if (now - lastActivity < timeout)
            {
                return;
            }

            _ = HandleConnectionLossAsync(new ConnectionException($"Heartbeat timed out after {timeout.TotalMilliseconds}ms"));
        }, null, interval, interval);
    }

    private void StopHeartbeat()
    {
        var timer = Interlocked.Exchange(ref _heartbeatTimer, null);
        timer?.Dispose();
    }

    private void MarkInboundActivity()
    {
        _lastInboundActivity = DateTimeOffset.UtcNow;
    }

    private void MarkOutboundActivity()
    {
        _lastOutboundActivity = DateTimeOffset.UtcNow;
    }

    private TimeSpan ResolveAsyncHandlerTimeout()
    {
        return _config.ResolvedAsyncHandlers.Timeout ?? Timeout;
    }

    private void OnAsyncHandlerError(Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.connection.handler_failed", new Dictionary<string, object?>
        {
            ["error"] = exception.Message,
        });
    }

    private void OnAsyncHandlerMetricsChanged(int activeCount, int queuedCount)
    {
        _config.Observability?.Meter?.Gauge("fitz.async_handlers.active", activeCount);
        _config.Observability?.Meter?.Gauge("fitz.async_handlers.queued", queuedCount);
    }

    private void OnAsyncHandlerSaturated(int activeCount, int queuedCount)
    {
        Log(FitzLogLevel.Warn, "fitz.connection.handler_saturated", new Dictionary<string, object?>
        {
            ["activeCount"] = activeCount,
            ["queuedCount"] = queuedCount,
        });
        _config.Observability?.Meter?.Counter("fitz.async_handlers.saturated", 1);
    }

    private void RecordRequestQueueFull()
    {
        Log(FitzLogLevel.Warn, "fitz.request_gate.full", new Dictionary<string, object?>
        {
            ["maxInFlightRequests"] = _config.MaxInFlightRequests,
            ["maxRequestQueueSize"] = _config.ResolvedMaxRequestQueueSize,
        });
        _config.Observability?.Meter?.Counter("fitz.request_gate.full", 1);
    }

    private void RecordRequestTimeout(ushort messageType)
    {
        Log(FitzLogLevel.Warn, "fitz.request.timeout", new Dictionary<string, object?>
        {
            ["messageType"] = messageType,
            ["timeoutMs"] = Timeout.TotalMilliseconds,
        });
        _config.Observability?.Meter?.Counter("fitz.request.timeout", 1);
    }

    private void RecordRetry(RetryOperation operation, int attempt, TimeSpan delay, Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.request.retry", new Dictionary<string, object?>
        {
            ["domain"] = operation.Domain,
            ["operation"] = operation.Operation,
            ["attempt"] = attempt,
            ["delayMs"] = delay.TotalMilliseconds,
            ["error"] = exception.Message,
        });
        _config.Observability?.Meter?.Counter("fitz.request.retry", 1);
    }

    private void RecordRetryExhausted(RetryOperation operation, int attempt, Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.request.retry_exhausted", new Dictionary<string, object?>
        {
            ["domain"] = operation.Domain,
            ["operation"] = operation.Operation,
            ["attempt"] = attempt,
            ["error"] = exception.Message,
        });
        _config.Observability?.Meter?.Counter("fitz.request.retry_exhausted", 1);
    }

    private void EmitLifecycleEvent(string eventName, Exception? exception = null, int? attempt = null)
    {
        _config.Observability?.OnLifecycleEvent?.Invoke(new FitzLifecycleEvent(
            eventName,
            State,
            _transport?.GetType().Name,
            _transport?.Url ?? _config.Url,
            attempt,
            exception?.Message));
    }

    private void Log(FitzLogLevel level, string eventName, IReadOnlyDictionary<string, object?>? fields = null)
    {
        _config.Observability?.Logger?.Log(level, eventName, fields);
    }

    private static bool ShouldRetry(RetryOperation operation, Exception exception)
    {
        return operation.RetryClass switch
        {
            RetryClass.WaitOnly => false,
            RetryClass.ReplayableRead => Retryability.IsRetryable(exception),
            RetryClass.ConfirmedNegativeRetry => exception is QueueException && Retryability.IsRetryable(exception),
            _ => false,
        };
    }

    private static string DescribeConnectionLoss(Exception exception)
    {
        return exception.Message.Length > 0
            ? exception.Message
            : "connection closed during CONNECT";
    }

    private TimeSpan GetDefaultAuthSettleDelay()
    {
        return TimeSpan.FromMilliseconds(100);
    }

    private static TimeSpan Min(TimeSpan left, TimeSpan right)
    {
        return left <= right ? left : right;
    }

    private static TaskCompletionSource<bool> CreateCompletedStateSignal()
    {
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        completed.TrySetResult(true);
        return completed;
    }

    private sealed class ReadyWaitRegistration : IDisposable
    {
        private readonly FitzConnection _owner;
        private int _disposed;

        internal ReadyWaitRegistration(FitzConnection owner)
        {
            _owner = owner;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            Interlocked.Decrement(ref _owner._readyWaiterCount);
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

    private sealed class DisconnectRegistration : IDisposable
    {
        private readonly FitzConnection _owner;
        private readonly long _listenerId;
        private int _disposed;

        internal DisconnectRegistration(FitzConnection owner, long listenerId)
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

            _owner.RemoveDisconnectListener(_listenerId);
        }
    }
}
