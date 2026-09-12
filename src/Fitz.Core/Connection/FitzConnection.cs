using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Connection;

/// <summary>
/// Owns the transport, the authenticated session, reconnection, and request correlation.
/// </summary>
/// <remarks>
/// Internal plumbing behind <see cref="Client"/>; applications use <c>IClient</c> instead.
/// Active subscriptions and worker registrations are restored after a reconnect.
/// </remarks>
sealed class FitzConnection : IAsyncDisposable
{
    static readonly TaskCompletionSource<bool> CompletedStateSignal = CreateCompletedStateSignal();
    static readonly CancellationToken ClosedConnectionToken = new(canceled: true);

    readonly object _gate = new();
    readonly ClientConfig _config;
    readonly Func<ITransport> _transportFactory;
    readonly Multiplexer _multiplexer;
    readonly FrameParser _frameParser;
    readonly Dictionary<long, Func<CancellationToken, ValueTask>> _reconnectListeners = [];
    readonly Dictionary<long, Action> _disconnectListeners = [];
    readonly List<CancellationTokenSource> _retiredConnectionClosedTokens = [];
    readonly Dictionary<string, AsyncHandlerDispatcher> _asyncHandlerDispatchers = new(StringComparer.Ordinal);
    readonly AsyncLocal<int> _restoreRequestDepth = new();
    CancellationTokenSource _connectionClosedCts = new();
    RequestGate _requestGate;
    TaskCompletionSource<bool> _stateChanged = CompletedStateSignal;

    ITransport? _transport;
    Task? _receiveLoop;
    CancellationTokenSource? _receiveLoopCts;
    TaskCompletionSource<bool>? _authFailure;
    Task? _connectTask;
    CancellationTokenSource? _connectCts;
    Task? _reconnectTask;
    Task? _connectionLossTask;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The reconnect token is disposed when its loop exits and defensively by DisposeAsync.")]
    CancellationTokenSource? _reconnectCts;
    volatile bool _authAttemptIsReconnect;
    volatile bool _closeRequested;
    volatile bool _authRejected;
    volatile bool _hasEstablishedSession;
    volatile bool _reconnectExhausted;
    long _nextReconnectListenerId;
    long _nextDisconnectListenerId;
    long _readyWaiterCount;
    volatile ConnectionState _state = ConnectionState.Disconnected;
    int _disposed;
    // Capabilities are per-session: a reconnect must observe a fresh SERVER_HELLO before the client
    // may correlate again, because the peer may not be the broker that answered last time. Packed
    // into a long so the receive loop can publish it atomically to request threads.
    long _capabilityState;
    long _nextCorrelationId;

    /// <summary>Creates a connection that builds its transport on demand.</summary>
    /// <param name="config">Configuration for the connection.</param>
    /// <param name="transportFactory">Creates a fresh transport for each connect attempt.</param>
    public FitzConnection(ClientConfig config, Func<ITransport> transportFactory)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _config.Validate();
        _transportFactory = transportFactory ?? throw new ArgumentNullException(nameof(transportFactory));
        _multiplexer = new Multiplexer(OnMultiplexerError, exception => ScheduleConnectionLoss(exception));
        _frameParser = new FrameParser(config.MaxFrameSize);
        _requestGate = CreateRequestGate();
    }

    /// <summary>The current connection lifecycle state.</summary>
    public ConnectionState State => _state;

    /// <summary>
    /// Whether the broker advertised <c>CAP_CORRELATION</c> for the current session.
    /// </summary>
    /// <remarks>
    /// False until a <c>SERVER_HELLO</c> arrives, and false for the whole session against a broker
    /// that never sends one. Requests issued before the advertisement are simply uncorrelated.
    /// </remarks>
    public bool CorrelationEnabled => Capabilities.SupportsCorrelation;

    /// <summary>The capabilities advertised for the current session.</summary>
    public ServerCapabilities Capabilities => UnpackCapabilities(Interlocked.Read(ref _capabilityState));

    static ServerCapabilities UnpackCapabilities(long packed) =>
        new((ushort)(packed >> 32), unchecked((uint)packed));

    static long PackCapabilities(ServerCapabilities capabilities) =>
        ((long)capabilities.ProtocolVersion << 32) | capabilities.CapabilityBits;

    /// <summary>
    /// Records a broker capability advertisement. An unparseable or unrecognised advertisement is
    /// ignored rather than rejected: it must never be more disruptive than a missing one.
    /// </summary>
    void ApplyServerHello(ReadOnlySpan<byte> payload)
    {
        if (!ServerCapabilities.TryParse(payload, out var capabilities))
        {
            return;
        }

        Interlocked.Exchange(ref _capabilityState, PackCapabilities(capabilities));
        Log(FitzLogLevel.Debug, "fitz.connection.server_hello", new Dictionary<string, object?>
        {
            ["protocol_version"] = capabilities.ProtocolVersion,
            ["correlation"] = capabilities.SupportsCorrelation,
        });
    }

    /// <summary>
    /// Whether a message type may carry a <c>CORRELATE</c> label.
    /// </summary>
    /// <remarks>
    /// RPC <c>REQUEST</c>/<c>RESPONSE</c> carry their own end-to-end 16-byte UUID and the broker
    /// does not additionally frame-correlate them, so labelling one would leave the caller waiting
    /// for a <c>CORRELATED</c> record that never arrives. They travel through
    /// <see cref="SendAsync"/> today and so never reach this path; the guard keeps that true if
    /// they are ever routed through a request instead.
    /// </remarks>
    static bool IsFrameCorrelatable(ushort messageType) =>
        messageType is not (MessageTypes.RpcRequest or MessageTypes.RpcResponse);

    /// <summary>
    /// Allocates an identifier that is unique among this connection's in-flight requests.
    /// Zero is reserved by the wire format and skipped.
    /// </summary>
    ulong NextCorrelationId()
    {
        while (true)
        {
            var next = unchecked((ulong)Interlocked.Increment(ref _nextCorrelationId));
            if (next != 0)
            {
                return next;
            }
        }
    }

    internal TimeSpan Timeout => _config.Timeout ?? TimeSpan.FromSeconds(30);
    internal int SubscriptionBufferCapacity => _config.ResolvedAsyncHandlers.SubscriptionBufferCapacity;
    internal CancellationToken ConnectionClosedToken => Volatile.Read(ref _disposed) == 0
        ? _connectionClosedCts.Token
        : ClosedConnectionToken;

    internal void InvalidateSession(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        _multiplexer.SetDisconnected();
        ScheduleConnectionLoss(exception);
    }

    internal bool TryDispatchAsyncHandler(
        string domain,
        Func<CancellationToken, ValueTask> handler,
        Action<Exception>? onRejected = null)
    {
        AsyncHandlerDispatcher dispatcher;
        lock (_gate)
        {
            if (_closeRequested || Volatile.Read(ref _disposed) != 0)
            {
                return false;
            }

            if (!_asyncHandlerDispatchers.TryGetValue(domain, out dispatcher!))
            {
                dispatcher = new AsyncHandlerDispatcher(
                    _config.ResolvedAsyncHandlers.MaxConcurrency,
                    _config.ResolvedAsyncHandlers.Timeout ?? System.Threading.Timeout.InfiniteTimeSpan,
                    Math.Max(_config.ResolvedAsyncHandlers.QueueCapacity, 0),
                    OnAsyncHandlerError,
                    onMetricsChanged: OnAsyncHandlerMetricsChanged,
                    onSaturated: OnAsyncHandlerSaturated);
                _asyncHandlerDispatchers.Add(domain, dispatcher);
            }
        }

        return dispatcher.TryDispatch(handler, onRejected);
    }

    internal void ReportAsyncHandlerError(Exception exception) => OnAsyncHandlerError(exception);

    internal async ValueTask<T> ExecuteWithRetryAsync<T>(
        RetryOperation operation,
        Func<CancellationToken, ValueTask<T>> task,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(task);

        var retry = _config.ResolvedRetry;
        if (!retry.Enabled || operation.RetryClass == RetryClass.WaitOnly)
        {
            return await task(ct).ConfigureAwait(false);
        }

        using var operationDeadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        operationDeadline.CancelAfter(Timeout);
        var operationToken = operationDeadline.Token;

        var attempts = 0;
        var delay = retry.Backoff ?? TimeSpan.FromMilliseconds(100);
        var maxDelay = retry.MaxBackoff ?? TimeSpan.FromSeconds(1);

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            attempts++;

            try
            {
                return await task(operationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException) when (operationDeadline.IsCancellationRequested)
            {
                throw new RequestTimeoutException($"{operation.Domain} {operation.Operation} exceeded the {Timeout} operation deadline");
            }
            catch (Exception ex) when (ShouldRetry(operation, ex) && attempts < retry.MaxAttempts)
            {
                var retryDelay = AddJitter(delay, maxDelay);
                RecordRetry(operation, attempts, retryDelay, ex);
                try
                {
                    await Task.Delay(retryDelay, operationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (OperationCanceledException)
                {
                    throw new RequestTimeoutException($"{operation.Domain} {operation.Operation} exceeded the {Timeout} operation deadline");
                }
                delay = NextBackoff(delay, maxDelay);
            }
            catch (Exception ex) when (ShouldRetry(operation, ex))
            {
                RecordRetryExhausted(operation, attempts, ex);
                throw;
            }
        }
    }

    /// <summary>Connects and authenticates, restoring registrations on a reconnect.</summary>
    /// <param name="ct">Cancellation token for the attempt.</param>
    /// <returns>A task that completes once the session is authenticated.</returns>
    public async Task ConnectAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

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
                var isReconnect = _hasEstablishedSession;
                _connectCts = new CancellationTokenSource();
                connectTask = TrackConnectTask(() => OpenAndAuthenticateAsync(isReconnect, _connectCts.Token));
            }
        }

        if (connectTask is not null)
        {
            await connectTask.WaitAsync(ct).ConfigureAwait(false);
            return;
        }

        await WaitUntilReadyAsync(Timeout, ct).ConfigureAwait(false);
    }

    internal async Task WaitUntilReadyAsync(TimeSpan waitTimeout, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var releaseWaiter = TryAcquireReadyWaitSlot();
        var deadline = waitTimeout == System.Threading.Timeout.InfiniteTimeSpan
            ? DateTimeOffset.MaxValue
            : DateTimeOffset.UtcNow + waitTimeout;

        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
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
                    await waitTask.WaitAsync(ct).ConfigureAwait(false);
                    continue;
                }

                var remaining = deadline - DateTimeOffset.UtcNow;
                if (remaining <= TimeSpan.Zero)
                {
                    throw new ConnectionException("Timed out waiting for connection to become ready");
                }

                await waitTask.WaitAsync(remaining, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            releaseWaiter?.Dispose();
        }
    }

    /// <summary>Sends a request and awaits its correlated response.</summary>
    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
    {
        await WaitForRequestReadyAsync(ct).ConfigureAwait(false);

        // Read the capability once: if the advertisement lands mid-request the decision must stay
        // consistent between the frame we encode and the way we register the request.
        var correlationId = CorrelationEnabled && IsFrameCorrelatable(messageType)
            ? NextCorrelationId()
            : 0UL;
        var frame = correlationId == 0
            ? FrameCodec.Encode(messageType, payload.Span)
            : FrameCodec.EncodeCorrelated(correlationId, messageType, payload.Span);
        ITransport? requestTransport = null;

        try
        {
            using var slot = await AcquireRequestSlotAsync(ct).ConfigureAwait(false);
            var transport = requestTransport = EnsureTransport();

            // Method group, not a lambda: the signatures match exactly, so this avoids a closure
            // and an extra async state machine on every request.
            var response = await _multiplexer.RequestAsync(
                messageType,
                frame,
                transport.SendAsync,
                Timeout,
                correlationId: correlationId,
                ct: ct).ConfigureAwait(false);

            return response;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (RequestTimeoutException)
        {
            RecordRequestTimeout(messageType);
            if (correlationId == 0)
            {
                ScheduleConnectionLoss(new ConnectionException(
                    $"Response lane {messageType} timed out and was reset to prevent response misdelivery."), requestTransport);
            }
            throw;
        }
        catch (RequestQueueFullException)
        {
            throw;
        }
        catch (ConnectionException ex)
        {
            ScheduleConnectionLoss(ex, requestTransport);
            throw;
        }
        catch (Exception ex)
        {
            ScheduleConnectionLoss(ex, requestTransport);
            throw new ConnectionException($"Request failed for message type {messageType}: {ex.Message}", ex);
        }
    }

    /// <summary>Sends a frame without awaiting a response.</summary>
    public async ValueTask SendAsync(
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
    {
        await WaitForRequestReadyAsync(ct).ConfigureAwait(false);
        var frame = FrameCodec.Encode(messageType, payload.Span);
        ITransport? sendTransport = null;

        try
        {
            using var slot = await AcquireRequestSlotAsync(ct).ConfigureAwait(false);
            sendTransport = EnsureTransport();
            await sendTransport.SendAsync(frame, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            ScheduleConnectionLoss(ex, sendTransport);
            throw;
        }
    }

    /// <summary>Routes broker-originated frames of one message type to a handler.</summary>
    /// <param name="messageType">Opcode from <see cref="Protocol.MessageTypes"/>.</param>
    /// <param name="handler">Receives each matching frame's payload.</param>
    /// <returns>A registration that stops delivery when disposed.</returns>
    public IDisposable RegisterNotificationHandler(ushort messageType, Action<byte[]> handler) => _multiplexer.RegisterNotificationHandler(messageType, handler);

    internal IDisposable RegisterBorrowedNotificationHandler(ushort messageType, Action<ReadOnlyMemory<byte>> handler) => _multiplexer.RegisterBorrowedNotificationHandler(messageType, handler);

    /// <summary>Registers a listener invoked after each successful reconnect.</summary>
    /// <param name="listener">Invoked once the session is authenticated again.</param>
    /// <returns>A registration that stops delivery when disposed.</returns>
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

    /// <summary>Registers a listener invoked when the connection is lost.</summary>
    /// <param name="listener">Invoked on disconnect.</param>
    /// <returns>A registration that stops delivery when disposed.</returns>
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

    /// <summary>Closes the connection and fails every pending request.</summary>
    /// <param name="ct">Cancellation token bounding the close.</param>
    /// <returns>A task that completes once the connection is closed.</returns>
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Connection teardown is best-effort and must complete after transport or receive-loop failures.")]
    public async Task CloseAsync(CancellationToken ct = default)
    {
        _closeRequested = true;
        Task? reconnectTask;
        Task? connectionLossTask;
        CancellationTokenSource? reconnectCts;
        CancellationTokenSource? connectCts;
        lock (_gate)
        {
            reconnectTask = _reconnectTask;
            connectionLossTask = _connectionLossTask;
            reconnectCts = _reconnectCts;
            connectCts = _connectCts;
        }
        await TryCancelAsync(reconnectCts).ConfigureAwait(false);
        await TryCancelAsync(connectCts).ConfigureAwait(false);
        var handlerDispatchers = SnapshotAsyncHandlerDispatchers();
        SetState(ConnectionState.Closed);
        NotifyDisconnect();
        foreach (var dispatcher in handlerDispatchers)
        {
            dispatcher.Close();
        }
        SignalConnectionClosed();
        _authFailure?.TrySetException(new ConnectionException("Connection closed"));
        _requestGate.Close();
        _multiplexer.SetDisconnected();
        if (_receiveLoopCts is not null)
        {
            await _receiveLoopCts.CancelAsync().ConfigureAwait(false);
        }

        var transport = DetachTransport();
        if (transport is not null)
        {
            try
            {
                await transport.CloseAsync(ct).WaitAsync(Timeout, ct).ConfigureAwait(false);
            }
            catch
            {
            }
        }

        if (_receiveLoop is not null)
        {
            try
            {
                await _receiveLoop.WaitAsync(Timeout, ct).ConfigureAwait(false);
            }
            catch
            {
            }

            _receiveLoop = null;
        }

        if (reconnectTask is not null && reconnectTask.Id != Task.CurrentId)
        {
            try
            {
                await reconnectTask.WaitAsync(Timeout, ct).ConfigureAwait(false);
            }
            catch
            {
            }
        }

        if (connectionLossTask is not null && connectionLossTask.Id != Task.CurrentId)
        {
            try
            {
                await connectionLossTask.WaitAsync(Timeout, ct).ConfigureAwait(false);
            }
            catch
            {
            }
        }

        try
        {
            await _multiplexer.CompleteNotificationDispatchAsync(Timeout, ct).ConfigureAwait(false);
        }
        catch
        {
        }

        if (transport is not null)
        {
            await DisposeTransportAsync(transport, ct).ConfigureAwait(false);
        }

        try
        {
            var drainTasks = new Task[handlerDispatchers.Length];
            for (var i = 0; i < handlerDispatchers.Length; i++)
            {
                drainTasks[i] = handlerDispatchers[i].DrainAsync();
            }

            await Task.WhenAll(drainTasks)
                .WaitAsync(Timeout, ct)
                .ConfigureAwait(false);
        }
        catch
        {
        }
        EmitLifecycleEvent("closed");
    }

    /// <summary>Closes the connection and releases transport resources.</summary>
    /// <returns>A task that completes once cleanup finishes.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await CloseAsync().ConfigureAwait(false);
        _receiveLoopCts?.Dispose();
        _connectCts?.Dispose();
        _reconnectCts?.Dispose();
        lock (_gate)
        {
            foreach (var retired in _retiredConnectionClosedTokens)
            {
                retired.Dispose();
            }

            _retiredConnectionClosedTokens.Clear();
        }
        _connectionClosedCts.Dispose();
        _multiplexer.Dispose();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "The background connect runner must transfer every failure to its tracked task.")]
    Task<bool> TrackConnectTask(Func<Task> operation)
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
                        _connectCts?.Dispose();
                        _connectCts = null;
                    }
                }
            }
        }
    }

    async Task WaitForRequestReadyAsync(CancellationToken ct)
    {
        if (State == ConnectionState.Authenticated ||
            (_restoreRequestDepth.Value > 0 && State == ConnectionState.Reconnecting))
        {
            return;
        }

        await WaitUntilReadyAsync(Timeout, ct).ConfigureAwait(false);
        EnsureAuthenticated();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Authentication failure handling must normalize and clean up every transport failure.")]
    async Task OpenAndAuthenticateAsync(bool isReconnect, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        ResetSessionState(isReconnect);
        Interlocked.Exchange(ref _requestGate, CreateRequestGate()).Close();
        SetState(isReconnect ? ConnectionState.Reconnecting : ConnectionState.Connecting);
        EmitLifecycleEvent(isReconnect ? "reconnect_start" : "connect_start");

        var transport = _transportFactory();
        var previousTransport = Interlocked.Exchange(ref _transport, transport);
        if (previousTransport is not null && !ReferenceEquals(previousTransport, transport))
        {
            await DisposeTransportAsync(previousTransport, CancellationToken.None).ConfigureAwait(false);
        }

        using var activity = FitzDiagnostics.ActivitySource.StartActivity(
            isReconnect ? "fitz.reconnect" : "fitz.connect",
            ActivityKind.Client);
        activity?.SetTag("server.address", transport.Url.Host);
        activity?.SetTag("network.transport", transport.TransportName);
        var customSpan = StartCustomSpan(isReconnect ? "fitz.reconnect" : "fitz.connect", transport);

        try
        {
            await transport.ConnectAsync(ct).ConfigureAwait(false);
            SetState(ConnectionState.Connected);
            _multiplexer.BeginSession();
            _frameParser.Reset();
            // A new transport session has not advertised anything yet. Until its SERVER_HELLO
            // arrives the client behaves as it would against a legacy broker.
            Interlocked.Exchange(ref _capabilityState, 0L);
            StartReceiveLoop();

            SetState(ConnectionState.Authenticating);
            _authAttemptIsReconnect = isReconnect;
            _authFailure = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var tokenProvider = _config.TokenProvider;
            var token = tokenProvider is null
                ? string.Empty
                : await tokenProvider(ct).ConfigureAwait(false);
            var tokenBytes = Encoding.UTF8.GetBytes(token);
            var connectFrame = FrameCodec.Encode(MessageTypes.Connect, tokenBytes);
            try
            {
                await transport.SendAsync(connectFrame, ct).ConfigureAwait(false);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(tokenBytes);
                CryptographicOperations.ZeroMemory(connectFrame);
            }

            await WaitForAuthSettlementAsync(ct).ConfigureAwait(false);
            await ThrowIfAttemptFailedAsync(transport).ConfigureAwait(false);

            if (isReconnect)
            {
                SetState(ConnectionState.Reconnecting);
            }

            RenewConnectionClosedToken();
            if (isReconnect)
            {
                _multiplexer.BeginNotificationRestore();
            }

            _multiplexer.SetConnected();
            _reconnectExhausted = false;

            if (isReconnect)
            {
                await RestoreReconnectStateAsync(ct).ConfigureAwait(false);
                _multiplexer.CompleteNotificationRestore();
            }

            await ThrowIfAttemptFailedAsync(transport).ConfigureAwait(false);

            _hasEstablishedSession = true;
            SetState(ConnectionState.Authenticated);
            EmitLifecycleEvent(isReconnect ? "reconnect_succeeded" : "connect_succeeded");
        }
        catch (Exception ex)
        {
            _multiplexer.AbortNotificationRestore();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            RecordCustomSpanException(customSpan, ex);
            _requestGate.Close();
            _multiplexer.SetDisconnected();

            var authFailure = ex is AuthenticationException
                ? ex
                : !isReconnect && _authRejected
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
                    await detached.CloseAsync(CancellationToken.None).WaitAsync(Timeout, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                }

                await DisposeTransportAsync(detached, CancellationToken.None).ConfigureAwait(false);
            }

            if (ex is OperationCanceledException && ct.IsCancellationRequested)
            {
                throw;
            }

            throw authFailure;
        }
        finally
        {
            FinishCustomSpan(customSpan);
            _authFailure = null;
            _authAttemptIsReconnect = false;
        }
    }

    async Task WaitForAuthSettlementAsync(CancellationToken ct)
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

        var delayTask = Task.Delay(settleDelay, ct);
        var completed = await Task.WhenAny(_authFailure.Task, delayTask).ConfigureAwait(false);
        if (completed == _authFailure.Task)
        {
            await _authFailure.Task.ConfigureAwait(false);
        }
    }

    async Task ThrowIfAttemptFailedAsync(ITransport transport)
    {
        var authFailure = _authFailure;
        if (authFailure?.Task.IsCompleted == true)
        {
            await authFailure.Task.ConfigureAwait(false);
        }

        if (!ReferenceEquals(Volatile.Read(ref _transport), transport))
        {
            throw new ConnectionException("Connection closed while establishing the session.");
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "The receive-loop boundary converts every unexpected transport or protocol failure into connection loss.")]
    void StartReceiveLoop()
    {
        _receiveLoopCts?.Cancel();
        _receiveLoopCts = new CancellationTokenSource();
        var token = _receiveLoopCts.Token;
        var receiveTransport = EnsureTransport();

        _receiveLoop = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested && !_closeRequested)
            {
                try
                {
                    using var data = await receiveTransport.ReceiveAsync(token).ConfigureAwait(false);
                    if (data.IsClosed)
                    {
                        throw new ConnectionException("Transport closed.");
                    }

                    if (!data.Memory.IsEmpty)
                    {
                        _frameParser.Append(data.Memory.Span);
                    }

                    DrainParsedFrames();
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested || _closeRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    ScheduleConnectionLoss(ex, receiveTransport);
                    return;
                }
            }
        }, token);
    }

    /// <summary>
    /// Reads every complete record the parser holds and routes it.
    /// </summary>
    /// <remarks>
    /// A <c>CORRELATED</c> record labels the record that immediately follows it in the same
    /// transport frame, so the two are read as a pair. A label with nothing after it is a protocol
    /// violation: the client's request-to-response mapping would be ambiguous, and there is no
    /// caller left to answer, so the session is torn down rather than guessed at.
    /// </remarks>
    void DrainParsedFrames()
    {
        while (_frameParser.TryReadFrame(out var frame))
        {
            if (frame.MessageType != MessageTypes.Correlated)
            {
                DispatchFrame(frame.MessageType, frame.Payload);
                continue;
            }

            var correlationId = FrameCodec.ReadCorrelationId(frame.Payload.Span);
            if (!_frameParser.TryReadFrame(out var labelled))
            {
                throw new ProtocolException(
                    $"Transport frame ended after a CORRELATED record for identifier {correlationId}.");
            }

            if (labelled.MessageType == MessageTypes.Correlated)
            {
                throw new ProtocolException("A CORRELATED record cannot label another CORRELATED record.");
            }

            if (!_multiplexer.DispatchCorrelated(correlationId, labelled.MessageType, labelled.Payload))
            {
                // The caller already gave up. Dropping is correct: handing this to any other waiter
                // is exactly the misdelivery correlation exists to prevent.
                RecordOrphanedCorrelatedResponse(labelled.MessageType);
            }
        }
    }

    void DispatchFrame(ushort messageType, ReadOnlyMemory<byte> payload)
    {
        if (messageType == MessageTypes.ServerHello)
        {
            ApplyServerHello(payload.Span);
            return;
        }

        _multiplexer.Dispatch(messageType, payload);
    }

    void ScheduleConnectionLoss(Exception exception, ITransport? failedTransport = null)
    {
        if (_closeRequested)
        {
            return;
        }

        lock (_gate)
        {
            if (_closeRequested || State is not ConnectionState.Authenticated and not ConnectionState.Authenticating and not ConnectionState.Reconnecting)
            {
                return;
            }

            if (failedTransport is not null && !ReferenceEquals(Volatile.Read(ref _transport), failedTransport))
            {
                return;
            }

            if (_connectionLossTask is { IsCompleted: false })
            {
                return;
            }

            _connectionLossTask = HandleConnectionLossSafelyAsync(exception, failedTransport);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Background connection-loss handling must never become an unobserved failure.")]
    async Task HandleConnectionLossSafelyAsync(Exception exception, ITransport? failedTransport)
    {
        try
        {
            await HandleConnectionLossAsync(exception, failedTransport).ConfigureAwait(false);
        }
        catch (Exception cleanupError)
        {
            Log(FitzLogLevel.Warn, "fitz.connection.loss_cleanup_failed", new Dictionary<string, object?>
            {
                ["error"] = cleanupError.Message,
            });
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Connection-loss teardown is best-effort and must continue after transport close failures.")]
    async Task HandleConnectionLossAsync(Exception exception, ITransport? failedTransport)
    {
        var transport = failedTransport is null
            ? DetachTransport()
            : ReferenceEquals(Interlocked.CompareExchange(ref _transport, null, failedTransport), failedTransport)
                ? failedTransport
                : null;
        if (failedTransport is not null && transport is null)
        {
            return;
        }

        SignalConnectionClosed();
        NotifyDisconnect();
        _requestGate.Close();
        _multiplexer.SetDisconnected();

        var stateAtLoss = State;

        if (_authAttemptIsReconnect)
        {
            _authFailure?.TrySetException(new ConnectionException(DescribeConnectionLoss(exception), exception));
        }
        else if (stateAtLoss == ConnectionState.Authenticating)
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

        SetState(_authRejected ? ConnectionState.Closed : ConnectionState.Disconnected);

        if (transport is not null)
        {
            try
            {
                await transport.CloseAsync().WaitAsync(Timeout).ConfigureAwait(false);
            }
            catch
            {
            }

            await DisposeTransportAsync(transport, CancellationToken.None).ConfigureAwait(false);
        }

        if (_authRejected)
        {
            EmitLifecycleEvent("auth_rejected", exception);
            return;
        }

        EmitLifecycleEvent("connection_lost", exception);

        var reconnect = _config.ResolvedReconnect;
        if (!reconnect.Enabled || !_hasEstablishedSession)
        {
            return;
        }

        EnsureReconnectLoop();
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect retries apply to every connection attempt failure except explicit shutdown or auth rejection.")]
    async Task ReconnectLoopAsync(CancellationToken ct)
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
                    await Task.Delay(AddJitter(delay, maxDelay), ct).ConfigureAwait(false);
                    if (_closeRequested)
                    {
                        return;
                    }

                    await OpenAndAuthenticateAsync(isReconnect: true, ct).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch when (!_closeRequested && !_authRejected)
                {
                    delay = NextBackoff(delay, maxDelay);
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

            if (State == ConnectionState.Disconnected)
            {
                EnsureReconnectLoop();
            }
        }
    }

    void EnsureReconnectLoop()
    {
        lock (_gate)
        {
            if (_reconnectTask is not null || !CanWaitForReconnectUnsafe())
            {
                return;
            }

            _reconnectCts?.Dispose();
            _reconnectCts = new CancellationTokenSource();
            _reconnectTask = ReconnectLoopAsync(_reconnectCts.Token);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect listeners are isolated so one consumer cannot prevent the others from restoring.")]
    async Task RestoreReconnectStateAsync(CancellationToken ct)
    {
        Func<CancellationToken, ValueTask>[] listeners;
        List<Exception>? failures = null;
        lock (_gate)
        {
            listeners = _reconnectListeners.Values.ToArray();
        }

        _restoreRequestDepth.Value++;
        try
        {
            foreach (var listener in listeners)
            {
                try
                {
                    await listener(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    Log(FitzLogLevel.Warn, "fitz.connection.reconnect_listener_failed", new Dictionary<string, object?>
                    {
                        ["error"] = exception.Message,
                    });
                    failures ??= [];
                    failures.Add(exception);
                }
            }
        }
        finally
        {
            _restoreRequestDepth.Value--;
        }

        if (failures is not null)
        {
            throw new AggregateException("One or more reconnect listeners failed to restore session state.", failures);
        }
    }

    void ResetSessionState(bool isReconnect)
    {
        _authRejected = false;
        if (!isReconnect)
        {
            _hasEstablishedSession = false;
        }
    }

    void RenewConnectionClosedToken()
    {
        CancellationTokenSource? previous;
        lock (_gate)
        {
            previous = _connectionClosedCts;
            _connectionClosedCts = new CancellationTokenSource();
            _retiredConnectionClosedTokens.Add(previous);
        }

        previous.Cancel();
    }

    void SignalConnectionClosed()
    {
        CancellationTokenSource current;
        lock (_gate)
        {
            current = _connectionClosedCts;
        }

        if (!current.IsCancellationRequested)
        {
            try
            {
                current.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Disconnect listeners are user callbacks and must be isolated from connection teardown.")]
    void NotifyDisconnect()
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

    void RemoveReconnectListener(long listenerId)
    {
        lock (_gate)
        {
            _reconnectListeners.Remove(listenerId);
        }
    }

    void RemoveDisconnectListener(long listenerId)
    {
        lock (_gate)
        {
            _disconnectListeners.Remove(listenerId);
        }
    }

    ITransport? DetachTransport() => Interlocked.Exchange(ref _transport, null);

    ITransport EnsureTransport() => Volatile.Read(ref _transport) ?? throw new ConnectionException("No active transport");

    void EnsureAuthenticated()
    {
        if (_closeRequested || State != ConnectionState.Authenticated)
        {
            throw new ConnectionException($"Cannot use connection while state is {Describe(State)}");
        }
    }

    RequestGate CreateRequestGate() => new(Math.Max(1, _config.MaxInFlightRequests), _config.ResolvedMaxRequestQueueSize);

    /// <summary>
    /// Acquires an in-flight request slot. Returns a completed <see cref="ValueTask{TResult}"/>
    /// when the gate is uncontended, which is the common case; the queue-full rejection is thrown
    /// synchronously by the gate, so no await is needed to observe it.
    /// </summary>
    ValueTask<RequestGate.Releaser> AcquireRequestSlotAsync(CancellationToken ct)
    {
        try
        {
            return _requestGate.AcquireAsync(ct);
        }
        catch (RequestQueueFullException)
        {
            RecordRequestQueueFull();
            throw;
        }
    }

    ReadyWaitRegistration? TryAcquireReadyWaitSlot()
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

    Exception? GetReadyFailure()
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

        return new ConnectionException($"Cannot use connection while state is {Describe(State)}");
    }

    bool CanWaitForReconnect()
    {
        lock (_gate)
        {
            return CanWaitForReconnectUnsafe();
        }
    }

    bool CanWaitForReconnectUnsafe()
    {
        var reconnect = _config.ResolvedReconnect;
        return reconnect.Enabled
            && _hasEstablishedSession
            && !_reconnectExhausted
            && !_authRejected
            && !_closeRequested;
    }

    void SetState(ConnectionState next)
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

    AsyncHandlerDispatcher[] SnapshotAsyncHandlerDispatchers()
    {
        lock (_gate)
        {
            return [.. _asyncHandlerDispatchers.Values];
        }
    }

    void OnAsyncHandlerError(Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.connection.handler_failed", new Dictionary<string, object?>
        {
            ["error"] = exception.Message,
        });
    }

    void OnMultiplexerError(Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.connection.notification_dispatch_failed", new Dictionary<string, object?>
        {
            ["error"] = exception.Message,
        });
    }

    void OnAsyncHandlerMetricsChanged(int activeCount, int queuedCount)
    {
        RecordCustomGauge("fitz.async_handlers.active", activeCount);
        RecordCustomGauge("fitz.async_handlers.queued", queuedCount);
    }

    void OnAsyncHandlerSaturated(int activeCount, int queuedCount)
    {
        Log(FitzLogLevel.Warn, "fitz.connection.handler_saturated", new Dictionary<string, object?>
        {
            ["activeCount"] = activeCount,
            ["queuedCount"] = queuedCount,
        });
        RecordCustomCounter("fitz.async_handlers.saturated");
        FitzDiagnostics.HandlerSaturated.Add(1);
    }

    void RecordRequestQueueFull()
    {
        Log(FitzLogLevel.Warn, "fitz.request_gate.full", new Dictionary<string, object?>
        {
            ["maxInFlightRequests"] = _config.MaxInFlightRequests,
            ["maxRequestQueueSize"] = _config.ResolvedMaxRequestQueueSize,
        });
        RecordCustomCounter("fitz.request_gate.full");
        FitzDiagnostics.RequestQueueFull.Add(1);
    }

    void RecordRequestTimeout(ushort messageType)
    {
        Log(FitzLogLevel.Warn, "fitz.request.timeout", new Dictionary<string, object?>
        {
            ["messageType"] = messageType,
            ["timeoutMs"] = Timeout.TotalMilliseconds,
        });
        RecordCustomCounter("fitz.request.timeout");
        FitzDiagnostics.RequestTimeouts.Add(1);
    }

    /// <summary>
    /// A correlated response arrived for a request nobody is waiting on any more, usually because
    /// the caller timed out or cancelled first. The frame is dropped, never rehomed.
    /// </summary>
    void RecordOrphanedCorrelatedResponse(ushort messageType)
    {
        Log(FitzLogLevel.Debug, "fitz.response.orphaned", new Dictionary<string, object?>
        {
            ["messageType"] = messageType,
        });
    }

    void RecordRetry(RetryOperation operation, int attempt, TimeSpan delay, Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.request.retry", new Dictionary<string, object?>
        {
            ["domain"] = operation.Domain,
            ["operation"] = operation.Operation,
            ["attempt"] = attempt,
            ["delayMs"] = delay.TotalMilliseconds,
            ["error"] = exception.Message,
        });
        RecordCustomCounter("fitz.request.retry");
        FitzDiagnostics.RequestRetries.Add(1);
    }

    void RecordRetryExhausted(RetryOperation operation, int attempt, Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.request.retry_exhausted", new Dictionary<string, object?>
        {
            ["domain"] = operation.Domain,
            ["operation"] = operation.Operation,
            ["attempt"] = attempt,
            ["error"] = exception.Message,
        });
        RecordCustomCounter("fitz.request.retry_exhausted");
        FitzDiagnostics.RetryExhausted.Add(1);
    }

    // Enum.ToString resolves names through runtime metadata; an explicit map keeps the
    // enum name tables trimmable.
    static string Describe(ConnectionState state) => state switch
    {
        ConnectionState.Disconnected => nameof(ConnectionState.Disconnected),
        ConnectionState.Connecting => nameof(ConnectionState.Connecting),
        ConnectionState.Connected => nameof(ConnectionState.Connected),
        ConnectionState.Reconnecting => nameof(ConnectionState.Reconnecting),
        ConnectionState.Authenticating => nameof(ConnectionState.Authenticating),
        ConnectionState.Authenticated => nameof(ConnectionState.Authenticated),
        ConnectionState.Closed => nameof(ConnectionState.Closed),
        _ => ((int)state).ToString(CultureInfo.InvariantCulture),
    };

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not alter connection lifecycle behavior.")]
    void EmitLifecycleEvent(string eventName, Exception? exception = null, int? attempt = null)
    {
        try
        {
            _config.Observability?.OnLifecycleEvent?.Invoke(new FitzLifecycleEvent(
                eventName,
                State,
                _transport?.TransportName,
                _transport?.Url ?? _config.Url,
                attempt,
                exception?.Message));
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not alter client behavior.")]
    void Log(FitzLogLevel level, string eventName, IReadOnlyDictionary<string, object?>? fields = null)
    {
        try
        {
            _config.Observability?.Logger?.Log(level, eventName, fields);
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not alter client behavior.")]
    IFitzSpan? StartCustomSpan(string name, ITransport transport)
    {
        try
        {
            return _config.Observability?.Tracer?.StartSpan(name, new Dictionary<string, object?>
            {
                ["server.address"] = transport.Url.Host,
                ["network.transport"] = transport.TransportName,
            });
        }
        catch
        {
            return null;
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not alter client behavior.")]
    static void RecordCustomSpanException(IFitzSpan? span, Exception exception)
    {
        try
        {
            span?.RecordException(exception);
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not alter client behavior.")]
    static void FinishCustomSpan(IFitzSpan? span)
    {
        try
        {
            span?.Finish();
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not alter client behavior.")]
    void RecordCustomCounter(string name)
    {
        try
        {
            _config.Observability?.Meter?.Counter(name, 1);
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not alter client behavior.")]
    void RecordCustomGauge(string name, double value)
    {
        try
        {
            _config.Observability?.Meter?.Gauge(name, value);
        }
        catch
        {
        }
    }

    static bool ShouldRetry(RetryOperation operation, Exception exception)
    {
        return operation.RetryClass switch
        {
            RetryClass.WaitOnly => false,
            RetryClass.ReplayableRead => Retryability.IsRetryable(exception),
            RetryClass.ConfirmedNegativeRetry => exception is QueueException && Retryability.IsRetryable(exception),
            _ => false,
        };
    }

    static string DescribeConnectionLoss(Exception exception)
    {
        return exception.Message.Length > 0
            ? exception.Message
            : "connection closed during CONNECT";
    }

    static TimeSpan GetDefaultAuthSettleDelay() => TimeSpan.FromMilliseconds(100);

    static TimeSpan Min(TimeSpan left, TimeSpan right) => left <= right ? left : right;

    static TimeSpan AddJitter(TimeSpan delay, TimeSpan maximum)
    {
        if (delay == System.Threading.Timeout.InfiniteTimeSpan)
        {
            return delay;
        }

        if (delay <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var factor = RandomNumberGenerator.GetInt32(800, 1201) / 1000d;
        var jitteredTicks = delay.Ticks > TimeSpan.MaxValue.Ticks / factor
            ? TimeSpan.MaxValue.Ticks
            : (long)(delay.Ticks * factor);
        var jittered = TimeSpan.FromTicks(jitteredTicks);
        return maximum == System.Threading.Timeout.InfiniteTimeSpan ? jittered : Min(jittered, maximum);
    }

    static TimeSpan NextBackoff(TimeSpan delay, TimeSpan maximum)
    {
        if (delay == System.Threading.Timeout.InfiniteTimeSpan)
        {
            return delay;
        }

        var doubled = delay.Ticks > TimeSpan.MaxValue.Ticks / 2
            ? TimeSpan.MaxValue
            : TimeSpan.FromTicks(delay.Ticks * 2);
        return maximum == System.Threading.Timeout.InfiniteTimeSpan ? doubled : Min(doubled, maximum);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Cancellation is best effort during concurrent lifecycle teardown.")]
    async Task TryCancelAsync(CancellationTokenSource? source)
    {
        if (source is null)
        {
            return;
        }

        try
        {
            await source.CancelAsync().WaitAsync(Timeout, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Transport disposal is best effort and bounded during lifecycle recovery.")]
    async Task DisposeTransportAsync(ITransport transport, CancellationToken ct)
    {
        try
        {
            await transport.DisposeAsync().AsTask().WaitAsync(Timeout, ct).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    static TaskCompletionSource<bool> CreateCompletedStateSignal()
    {
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        completed.TrySetResult(true);
        return completed;
    }

    sealed class ReadyWaitRegistration : IDisposable
    {
        readonly FitzConnection _owner;
        int _disposed;

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

    sealed class ReconnectRegistration : IDisposable
    {
        readonly FitzConnection _owner;
        readonly long _listenerId;
        int _disposed;

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

    sealed class DisconnectRegistration : IDisposable
    {
        readonly FitzConnection _owner;
        readonly long _listenerId;
        int _disposed;

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
