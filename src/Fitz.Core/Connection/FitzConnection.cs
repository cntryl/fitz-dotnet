using System.Diagnostics;
using System.Reflection;
using System.Security.Cryptography;
using Cntryl.Fitz.Observability;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Connection;

/// <summary>
/// Owns the transport, the authenticated session, reconnection, and request correlation.
/// </summary>
/// <remarks>
/// Internal plumbing behind <see cref="FitzClient"/>; applications use <c>IFitzClient</c> instead.
/// Active subscriptions and worker registrations are restored after a reconnect.
/// </remarks>
sealed class FitzConnection : IAsyncDisposable
{
    const ushort UnauthorizedCode = 1;
    const ushort VersionMismatchCode = 50;
    static readonly TaskCompletionSource<bool> CompletedStateSignal = CreateCompletedStateSignal();
    static readonly CancellationToken ClosedConnectionToken = new(canceled: true);

    readonly object _gate = new();
    readonly ClientConfig _config;
    readonly Func<ITransport> _transportFactory;
    readonly Multiplexer _multiplexer;
    readonly ConnectionTelemetry _telemetry;
    readonly ListenerRegistry<Func<CancellationToken, ValueTask>> _reconnectListeners = new();
    readonly ListenerRegistry<Action> _disconnectListeners = new();
    readonly Dictionary<string, AsyncHandlerDispatcher> _asyncHandlerDispatchers = [with(StringComparer.Ordinal)];
    readonly AsyncLocal<int> _restoreRequestDepth = new();
    internal event Action<Exception>? NotificationDeliveryFailed;
    CancellationTokenSource _connectionClosedCts;
    CancellationToken _connectionClosedToken;
    RequestGate _requestGate;
    TaskCompletionSource<bool> _stateChanged = CompletedStateSignal;

    ITransport? _transport;
    ITransport? _goAwayTransport;
    Task? _receiveLoop;
    CancellationTokenSource? _receiveLoopCts;
    TaskCompletionSource<bool>? _authFailure;
    Task? _connectTask;
    CancellationTokenSource? _connectCts;
    Task? _reconnectTask;
    Task? _connectionLossTask;
    CancellationTokenSource? _reconnectCts;
    volatile bool _authAttemptIsReconnect;
    volatile bool _closeRequested;
    volatile bool _authRejected;
    volatile bool _hasEstablishedSession;
    volatile bool _reconnectExhausted;
    long _readyWaiterCount;
    volatile ConnectionState _state = ConnectionState.Disconnected;
    int _disposed;
    // Negotiated limits and session identifiers are per-session and published as one immutable object.
    ServerCapabilities? _capabilities;
    long _nextCorrelationId;

    /// <summary>Creates a connection that builds its transport on demand.</summary>
    /// <param name="config">Configuration for the connection.</param>
    /// <param name="transportFactory">Creates a fresh transport for each connect attempt.</param>
    public FitzConnection(ClientConfig config, Func<ITransport> transportFactory)
        : this(config, transportFactory, TimeProvider.System)
    {
    }

    /// <summary>Creates a connection that builds its transport on demand and keeps time with <paramref name="time"/>.</summary>
    /// <param name="config">Configuration for the connection.</param>
    /// <param name="transportFactory">Creates a fresh transport for each connect attempt.</param>
    /// <param name="time">The clock for timeouts, retry and reconnect backoff, and domain timers.</param>
    internal FitzConnection(ClientConfig config, Func<ITransport> transportFactory, TimeProvider time)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _config.Validate();
        _transportFactory = transportFactory ?? throw new ArgumentNullException(nameof(transportFactory));
        Time = time ?? throw new ArgumentNullException(nameof(time));
        _telemetry = new ConnectionTelemetry(_config, () => _state, () => Volatile.Read(ref _transport));
        _multiplexer = new Multiplexer(
            OnMultiplexerError,
            exception => ScheduleConnectionLoss(exception),
            timeProvider: time);
        _requestGate = CreateRequestGate();
        _connectionClosedCts = new CancellationTokenSource();
        _connectionClosedToken = _connectionClosedCts.Token;
    }

    /// <summary>The current connection lifecycle state.</summary>
    public ConnectionState State => _state;

    /// <summary>
    /// Whether the Wire 3 handshake completed for the current session.
    /// </summary>
    /// <remarks>
    /// False until the broker sends its Wire 3 <c>HELLO</c> frame.
    /// </remarks>

    /// <summary>The capabilities advertised for the current session.</summary>
    public ServerCapabilities Capabilities => Volatile.Read(ref _capabilities) ?? ServerCapabilities.None;

    /// <summary>
    /// Validates the broker HELLO and completes authentication for its transport generation.
    /// </summary>
    ValueTask ApplyHelloAsync(ReadOnlyMemory<byte> payload, uint capabilityMask, ITransport sessionTransport)
    {
        if (!ServerCapabilities.TryParse(payload.Span, out var capabilities))
        {
            throw new ProtocolException("HELLO payload is truncated or contains invalid negotiated limits.");
        }
        capabilities = capabilities with { CapabilityMask = capabilityMask };

        TaskCompletionSource<bool>? authentication;
        lock (_gate)
        {
            if (!ReferenceEquals(Volatile.Read(ref _transport), sessionTransport))
            {
                return ValueTask.CompletedTask;
            }

            if (Volatile.Read(ref _capabilities) is not null)
            {
                throw new ProtocolException("Repeated HELLO cannot renegotiate an established session.");
            }

            Volatile.Write(ref _capabilities, capabilities);
            var inFlightLimit = Math.Min(capabilities.MaxInFlightRequests, (uint)_config.MaxInFlightRequests);
            _requestGate.SetMaxConcurrency((int)inFlightLimit);
            authentication = _authFailure;
        }
        authentication?.TrySetResult(true);
        _telemetry.Log(FitzLogLevel.Debug, "fitz.connection.hello", new Dictionary<string, object?>
        {
            ["protocol_version"] = capabilities.ProtocolVersion,
            ["max_message_bytes"] = capabilities.MaxMessageBytes,
            ["max_in_flight"] = capabilities.MaxInFlightRequests,
            ["session_id"] = capabilities.SessionId,
        });
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Whether an outbound message carries its own asynchronous RPC routing semantics.
    /// </summary>
    /// <remarks>
    /// RPC protocol messages are sent without awaiting a client-side response, but still carry a
    /// nonzero Wire 3 frame correlation identifier.
    /// </remarks>
    static bool IsRpcMessage(ushort messageType) => messageType is >= 300 and <= 305;

    /// <summary>
    /// Allocates an identifier that is unique among this connection's in-flight requests.
    /// Zero is reserved by the wire format and skipped.
    /// </summary>
    ulong NextCorrelationId()
    {
        while (true)
        {
            var current = Volatile.Read(ref _nextCorrelationId);
            if (current == -1)
            {
                throw new ProtocolException("Correlation identifiers exhausted; reconnect before another request.");
            }
            var next = unchecked(current + 1);
            if (Interlocked.CompareExchange(ref _nextCorrelationId, next, current) == current)
                return unchecked((ulong)next);
        }
    }

    static readonly TimeSpan DefaultRetryBackoff = TimeSpan.FromMilliseconds(100);
    static readonly TimeSpan DefaultRetryMaxBackoff = TimeSpan.FromSeconds(1);
    static readonly TimeSpan DefaultReconnectBackoff = TimeSpan.FromMilliseconds(250);
    static readonly TimeSpan DefaultReconnectMaxBackoff = TimeSpan.FromSeconds(5);
    static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    internal TimeSpan Timeout => _config.Timeout ?? DefaultTimeout;

    /// <summary>The clock for timeouts, backoff, and the timers of domain clients built on this connection.</summary>
    internal TimeProvider Time { get; }

    // The configured timeout plus time the broker is known to hold the request, saturating
    // to infinite rather than overflowing.
    TimeSpan ResponseTimeout(TimeSpan extra)
    {
        var timeout = Timeout;
        if (extra <= TimeSpan.Zero)
            return timeout;
        if (timeout == System.Threading.Timeout.InfiniteTimeSpan || extra >= TimeSpan.MaxValue - timeout)
            return System.Threading.Timeout.InfiniteTimeSpan;
        return timeout + extra;
    }
    internal int SubscriptionBufferCapacity => _config.ResolvedAsyncHandlers.SubscriptionBufferCapacity;
    // Read from a field rather than through the source: a retired source is disposed once
    // cancelled, and CancellationTokenSource.Token throws after disposal.
    internal CancellationToken ConnectionClosedToken => Volatile.Read(ref _disposed) == 0
        ? _connectionClosedToken
        : ClosedConnectionToken;

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
                    _telemetry.HandlerFailed,
                    onMetricsChanged: _telemetry.HandlerMetricsChanged,
                    onSaturated: _telemetry.HandlerSaturated,
                    timeProvider: Time);
                _asyncHandlerDispatchers.Add(domain, dispatcher);
            }
        }

        return dispatcher.TryDispatch(handler, onRejected);
    }

    internal void ReportAsyncHandlerError(Exception exception) => _telemetry.HandlerFailed(exception);

    /// <summary>Sends a request, retrying it under <paramref name="operation"/>'s retry policy.</summary>
    internal ValueTask<ReadOnlyMemory<byte>> RequestWithRetryAsync(
        RetryOperation operation,
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct) =>
        ExecuteWithRetryAsync(operation, token => RequestAsync(messageType, payload, token), ct);

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

        using var operationDeadline = new CancellationTokenSource(Timeout, Time);
        using var operationScope = CancellationTokenSource.CreateLinkedTokenSource(ct, operationDeadline.Token);
        var operationToken = operationScope.Token;

        var attempts = 0;
        var backoff = new ExponentialBackoff(retry.Backoff ?? DefaultRetryBackoff, retry.MaxBackoff ?? DefaultRetryMaxBackoff);

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
            catch (Exception ex) when (operation.ShouldRetry(ex) && attempts < retry.MaxAttempts)
            {
                var retryDelay = backoff.Next();
                _telemetry.Retrying(operation, attempts, retryDelay, ex);
                try
                {
                    await Task.Delay(retryDelay, Time, operationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (OperationCanceledException)
                {
                    throw new RequestTimeoutException($"{operation.Domain} {operation.Operation} exceeded the {Timeout} operation deadline");
                }
            }
            catch (Exception ex) when (operation.ShouldRetry(ex))
            {
                _telemetry.RetryExhausted(operation, attempts, ex);
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
            : Time.GetUtcNow() + waitTimeout;

        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                // SetState publishes the state and swaps the signal under _gate, so reading both
                // here under the same lock cannot miss a transition between check and wait.
                Exception? failure;
                Task waitTask;
                lock (_gate)
                {
                    if (State == ConnectionState.Authenticated)
                    {
                        return;
                    }

                    failure = GetReadyFailure();
                    waitTask = _stateChanged.Task;
                }

                if (failure is not null)
                {
                    throw failure;
                }

                if (waitTimeout == System.Threading.Timeout.InfiniteTimeSpan)
                {
                    await waitTask.WaitAsync(ct).ConfigureAwait(false);
                    continue;
                }

                var remaining = deadline - Time.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                {
                    throw new ConnectionException("Timed out waiting for connection to become ready");
                }

                await waitTask.WaitAsync(remaining, Time, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            releaseWaiter?.Dispose();
        }
    }

    /// <summary>Sends a request and awaits its correlated response.</summary>
    public ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default) => RequestWithFlagsAsync(messageType, payload, 0, ct);

    /// <summary>Sends a request with Wire 3 frame flags and awaits its correlated response.</summary>
    internal async ValueTask<ReadOnlyMemory<byte>> RequestWithFlagsAsync(
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        byte flags,
        CancellationToken ct = default) =>
        (await RequestInSessionCoreAsync(messageType, payload, flags, expectedSession: null, TimeSpan.Zero, ct).ConfigureAwait(false)).Payload;

    internal ValueTask<SessionResponse> RequestInSessionAsync(
        ushort messageType, ReadOnlyMemory<byte> payload, TimeSpan extraResponseTime = default, CancellationToken ct = default) =>
        RequestInSessionCoreAsync(messageType, payload, 0, expectedSession: null, extraResponseTime, ct);

    internal async ValueTask<ReadOnlyMemory<byte>> RequestOnSessionAsync(
        ushort messageType, ReadOnlyMemory<byte> payload, CancellationToken session, CancellationToken ct) =>
        (await RequestInSessionCoreAsync(messageType, payload, 0, session, TimeSpan.Zero, ct).ConfigureAwait(false)).Payload;

    async ValueTask<SessionResponse> RequestInSessionCoreAsync(
        ushort messageType, ReadOnlyMemory<byte> payload, byte flags,
        CancellationToken? expectedSession, TimeSpan extraResponseTime, CancellationToken ct)
    {
        if (expectedSession?.IsCancellationRequested == true)
            throw new ConnectionException("The owning connection session has ended.");
        await WaitForRequestReadyAsync(ct).ConfigureAwait(false);

        using var slot = await AcquireRequestSlotAsync(ct).ConfigureAwait(false);
        var (requestTransport, session) = CaptureTransportSession(expectedSession);
        var correlationId = NextCorrelationId();
        var frame = EncodeFrame(messageType, payload.Span, flags, correlationId);

        try
        {
            // Method group, not a lambda: the signatures match exactly, so this avoids a closure
            // and an extra async state machine on every request.
            var response = await _multiplexer.RequestAsync(
                messageType,
                frame,
                requestTransport.SendAsync,
                ResponseTimeout(extraResponseTime),
                correlationId,
                ct).ConfigureAwait(false);

            return new SessionResponse(response, session);
        }
        catch (TransportSendCanceledException exception)
        {
            ScheduleConnectionLoss(exception, requestTransport);
            throw;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (RequestTimeoutException)
        {
            _telemetry.RequestTimedOut(messageType, Timeout);
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
        CancellationToken ct = default) =>
        _ = await SendInSessionAsync(messageType, payload, ct).ConfigureAwait(false);

    internal async ValueTask<CancellationToken> SendInSessionAsync(
        ushort messageType, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        await WaitForRequestReadyAsync(ct).ConfigureAwait(false);
        using var slot = await AcquireRequestSlotAsync(ct).ConfigureAwait(false);
        var (sendTransport, session) = CaptureTransportSession(expectedSession: null);
        var rpcResponseWithoutRequestCorrelation = messageType is 303 or 305 ||
            (messageType == 304 && (payload.IsEmpty || payload.Span[0] != 1));
        var correlationId = IsRpcMessage(messageType) && !rpcResponseWithoutRequestCorrelation
            ? NextCorrelationId()
            : 0UL;
        var frame = EncodeFrame(messageType, payload.Span, 0, correlationId);
        try
        {
            await sendTransport.SendAsync(frame, ct).ConfigureAwait(false);
            return session;
        }
        catch (TransportSendCanceledException exception)
        {
            ScheduleConnectionLoss(exception, sendTransport);
            throw;
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

    byte[] EncodeFrame(ushort messageType, ReadOnlySpan<byte> payload, byte flags, ulong correlationId)
    {
        var frame = FrameCodec.Encode(messageType, payload, flags, correlationId);
        var negotiatedLimit = EffectiveFrameLimit;
        if (frame.Length > negotiatedLimit)
        {
            throw new ProtocolException($"Encoded frame length {frame.Length} exceeds configured or negotiated limit {negotiatedLimit}.");
        }
        return frame;
    }

    int EffectiveFrameLimit
    {
        get
        {
            var capabilities = Capabilities;
            return capabilities.ProtocolVersion >= 2
                ? Math.Min(_config.MaxFrameSize, checked((int)capabilities.MaxMessageBytes))
                : _config.MaxFrameSize;
        }
    }

    /// <summary>Routes broker-originated frames of one message type to a handler.</summary>
    /// <param name="messageType">Opcode from <see cref="Protocol.MessageTypes"/>.</param>
    /// <param name="handler">
    /// Receives each matching frame's payload. The memory is a copy the handler owns and may retain.
    /// </param>
    /// <returns>A registration that stops delivery when disposed.</returns>
    internal IDisposable RegisterNotificationHandler(ushort messageType, Action<ReadOnlyMemory<byte>> handler) =>
        _multiplexer.RegisterNotificationHandler(messageType, handler);

    /// <summary>Registers a listener invoked after each successful reconnect.</summary>
    /// <param name="listener">Invoked once the session is authenticated again.</param>
    /// <returns>A registration that stops delivery when disposed.</returns>
    public IDisposable OnReconnect(Func<CancellationToken, ValueTask> listener) => _reconnectListeners.Add(listener);

    /// <summary>Registers a listener invoked when the connection is lost.</summary>
    /// <param name="listener">Invoked on disconnect.</param>
    /// <returns>A registration that stops delivery when disposed.</returns>
    public IDisposable OnDisconnect(Action listener) => _disconnectListeners.Add(listener);

    /// <summary>Closes the connection and fails every pending request.</summary>
    /// <param name="ct">Cancellation token bounding the close.</param>
    /// <returns>A task that completes once the connection is closed.</returns>
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
                await transport.CloseAsync(ct).WaitAsync(Timeout, Time, ct).ConfigureAwait(false);
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
            }
        }

        if (_receiveLoop is not null)
        {
            try
            {
                await _receiveLoop.WaitAsync(Timeout, Time, ct).ConfigureAwait(false);
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
            }

            _receiveLoop = null;
        }

        if (reconnectTask is not null && reconnectTask.Id != Task.CurrentId)
        {
            try
            {
                await reconnectTask.WaitAsync(Timeout, Time, ct).ConfigureAwait(false);
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
            }
        }

        if (connectionLossTask is not null && connectionLossTask.Id != Task.CurrentId)
        {
            try
            {
                await connectionLossTask.WaitAsync(Timeout, Time, ct).ConfigureAwait(false);
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
            }
        }

        try
        {
            await _multiplexer.CompleteNotificationDispatchAsync(Timeout, ct).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
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
                .WaitAsync(Timeout, Time, ct)
                .ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
        }
        _telemetry.LifecycleEvent("closed");
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
        _connectionClosedCts.Dispose();
        _multiplexer.Dispose();
    }

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
            catch (Exception ex) when (ExceptionPolicy.IsRecoverable(ex))
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

    async Task OpenAndAuthenticateAsync(bool isReconnect, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        ResetSessionState(isReconnect);
        Interlocked.Exchange(ref _requestGate, CreateRequestGate()).Close();
        SetState(isReconnect ? ConnectionState.Reconnecting : ConnectionState.Connecting);
        _telemetry.LifecycleEvent(isReconnect ? "reconnect_start" : "connect_start");

        var transport = _transportFactory();
        ITransport? previousTransport;
        lock (_gate)
        {
            Volatile.Write(ref _capabilities, null);
            _authFailure = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _goAwayTransport = null;
            previousTransport = Interlocked.Exchange(ref _transport, transport);
        }
        if (previousTransport is not null && !ReferenceEquals(previousTransport, transport))
        {
            await DisposeTransportAsync(previousTransport, CancellationToken.None).ConfigureAwait(false);
        }

        using var activity = FitzDiagnostics.ActivitySource.StartActivity(
            isReconnect ? "fitz.reconnect" : "fitz.connect",
            ActivityKind.Client);
        activity?.SetTag("server.address", transport.Url.Host);
        activity?.SetTag("network.transport", transport.TransportName);
        var customSpan = _telemetry.StartSpan(isReconnect ? "fitz.reconnect" : "fitz.connect", transport);

        try
        {
            await transport.ConnectAsync(ct).ConfigureAwait(false);
            SetState(ConnectionState.Connected);
            Interlocked.Exchange(ref _nextCorrelationId, 0);
            SetState(ConnectionState.Authenticating);
            _authAttemptIsReconnect = isReconnect;
            StartReceiveLoop();

            var tokenProvider = _config.TokenProvider;
            var token = tokenProvider is null
                ? string.Empty
                : await tokenProvider(ct).ConfigureAwait(false);
            var clientVersion = typeof(FitzConnection).Assembly
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";
            byte[] connectPayload;
            using (var writer = new BinaryBufferWriter())
            {
                writer.WriteU16(3);
                writer.WriteU16(3);
                writer.WriteU32(0);
                writer.WriteString(token);
                writer.WriteString(_config.ServiceName?.Trim() ?? string.Empty);
                writer.WriteString($"fitz-dotnet/{clientVersion}");
                connectPayload = writer.Build();
            }
            connectPayload = ExtensionCodec.Encode(connectPayload, 1, ExtensionCodec.ClientCapabilities);
            var connectFrame = EncodeFrame(MessageTypes.Connect, connectPayload, ExtensionCodec.PresentFlag, 0);
            try
            {
                await transport.SendAsync(connectFrame, ct).ConfigureAwait(false);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(connectPayload);
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
            _telemetry.LifecycleEvent(isReconnect ? "reconnect_succeeded" : "connect_succeeded");
        }
        catch (Exception ex) when (ExceptionPolicy.IsRecoverable(ex))
        {
            _multiplexer.AbortNotificationRestore();
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            ConnectionTelemetry.RecordSpanException(customSpan, ex);
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
                _telemetry.LifecycleEvent("auth_rejected", authFailure);
            }
            else if (_closeRequested)
            {
                SetState(ConnectionState.Closed);
            }
            else
            {
                SetState(ConnectionState.Disconnected);
                _telemetry.LifecycleEvent(isReconnect ? "reconnect_failed" : "connect_failed", authFailure);
            }

            var detached = DetachTransport();
            if (detached is not null)
            {
                try
                {
                    await detached.CloseAsync(CancellationToken.None).WaitAsync(Timeout, Time, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
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
            ConnectionTelemetry.FinishSpan(customSpan);
            _authFailure = null;
            _authAttemptIsReconnect = false;
        }
    }

    async Task WaitForAuthSettlementAsync(CancellationToken ct)
    {
        var authentication = _authFailure
            ?? throw new ConnectionException("Connection closed during the Wire 3 handshake.");
        try
        {
            await authentication.Task.WaitAsync(Timeout, Time, ct).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new RequestTimeoutException("Timed out waiting for the broker HELLO.", exception);
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

                    if (!ReferenceEquals(Volatile.Read(ref _transport), receiveTransport))
                    {
                        return;
                    }

                    if (!data.Memory.IsEmpty)
                    {
                        if (data.Memory.Length > EffectiveFrameLimit)
                        {
                            throw new ProtocolException($"Received frame length {data.Memory.Length} exceeds configured or negotiated limit {EffectiveFrameLimit}.");
                        }
                        var frame = FrameCodec.DecodeStrict(data.Memory);
                        await DispatchFrameAsync(frame, receiveTransport).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested || _closeRequested)
                {
                    return;
                }
                catch (Exception ex) when (ExceptionPolicy.IsRecoverable(ex))
                {
                    ScheduleConnectionLoss(ex, receiveTransport);
                    return;
                }
            }
        }, token);
    }

    async ValueTask DispatchFrameAsync(Frame frame, ITransport sessionTransport)
    {
        var decoded = ExtensionCodec.Decode(frame);
        frame = frame with { Payload = decoded.Core };
        if (frame.MessageType is MessageTypes.Hello or MessageTypes.ConnectRejected or MessageTypes.GoAway)
        {
            if (frame.CorrelationId != 0 || (frame.Flags & ~ExtensionCodec.PresentFlag) != 0)
            {
                throw new ProtocolException("Wire 3 control frames must have zero flags and correlation.");
            }

            if (frame.MessageType == MessageTypes.Hello)
            {
                await ApplyHelloAsync(frame.Payload, decoded.CapabilityMask, sessionTransport).ConfigureAwait(false);
                return;
            }

            if (frame.MessageType == MessageTypes.ConnectRejected)
            {
                var rejected = new BinaryBufferReader(frame.Payload);
                var code = rejected.ReadU16();
                var message = rejected.ReadString();
                if (!rejected.IsEof)
                {
                    throw new ProtocolException("CONNECT_REJECTED contains trailing bytes.");
                }
                throw code switch
                {
                    UnauthorizedCode => new AuthenticationException($"Broker rejected CONNECT ({code}): {message}", code),
                    VersionMismatchCode => new ProtocolException($"Wire version mismatch ({code}): {message}", code),
                    _ => new ProtocolException($"Broker rejected CONNECT ({code}): {message}", code),
                };
            }

            var goAway = new BinaryBufferReader(frame.Payload);
            var goAwayCode = goAway.ReadU16();
            var graceMs = goAway.ReadU32();
            var reason = goAway.ReadString();
            if (!goAway.IsEof)
            {
                throw new ProtocolException("GOAWAY contains trailing bytes.");
            }
            _telemetry.Log(FitzLogLevel.Info, "fitz.connection.goaway", new Dictionary<string, object?>
            {
                ["code"] = goAwayCode,
                ["grace_ms"] = graceMs,
                ["reason"] = reason,
            });
            _telemetry.LifecycleEvent("goaway", code: goAwayCode, graceMs: graceMs);
            var startGoAway = false;
            lock (_gate)
            {
                if (!ReferenceEquals(_goAwayTransport, sessionTransport))
                {
                    _goAwayTransport = sessionTransport;
                    startGoAway = true;
                }
            }
            if (startGoAway && (goAwayCode == 55 || graceMs == 0))
            {
                ScheduleConnectionLoss(new ConnectionException($"Broker sent GOAWAY ({goAwayCode}): {reason}"), sessionTransport);
            }
            else if (startGoAway)
            {
                var drainToken = _receiveLoopCts?.Token ?? CancellationToken.None;
                _ = EndAfterGoAwayGraceAsync(sessionTransport, graceMs, goAwayCode, reason, drainToken);
            }
            return;
        }

        if (frame.CorrelationId != 0)
        {
            _multiplexer.DispatchCorrelated(
                frame.CorrelationId,
                frame.Payload,
                (frame.Flags & FrameCodec.ResponseFinalFlag) != 0);
            return;
        }

        _multiplexer.DispatchNotification(frame.MessageType, frame.Payload);
    }

    async Task EndAfterGoAwayGraceAsync(ITransport sessionTransport, uint graceMs, ushort code, string reason, CancellationToken ct)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(graceMs), Time, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }
        ScheduleConnectionLoss(new ConnectionException($"Broker sent GOAWAY ({code}): {reason}"), sessionTransport);
    }

    void ScheduleConnectionLoss(Exception exception, ITransport? failedTransport = null)
    {
        if (_closeRequested)
        {
            return;
        }

        TaskCompletionSource lossCompletion;
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

            lossCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _connectionLossTask = lossCompletion.Task;
        }

        // Teardown cancels session tokens and notifies disconnect listeners synchronously.
        // Those are caller callbacks, so they must not run while _gate is held.
        _ = HandleConnectionLossSafelyAsync(exception, failedTransport, lossCompletion);
    }

    async Task HandleConnectionLossSafelyAsync(Exception exception, ITransport? failedTransport, TaskCompletionSource completion)
    {
        try
        {
            await HandleConnectionLossAsync(exception, failedTransport).ConfigureAwait(false);
        }
        catch (Exception cleanupError) when (ExceptionPolicy.IsRecoverable(cleanupError))
        {
            _telemetry.Log(FitzLogLevel.Warn, "fitz.connection.loss_cleanup_failed", new Dictionary<string, object?>
            {
                ["error"] = cleanupError.Message,
                ["error_detail"] = cleanupError.ToString(),
            });
        }
        finally
        {
            completion.TrySetResult();
        }
    }

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

        if (exception is ProtocolException { RejectionCode: not null })
        {
            _authFailure?.TrySetException(exception);
        }
        else if (_authAttemptIsReconnect)
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
                await transport.CloseAsync().WaitAsync(Timeout, Time).ConfigureAwait(false);
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
            }

            await DisposeTransportAsync(transport, CancellationToken.None).ConfigureAwait(false);
        }

        if (_authRejected)
        {
            _telemetry.LifecycleEvent("auth_rejected", exception);
            return;
        }

        _telemetry.LifecycleEvent("connection_lost", exception);

        var reconnect = _config.ResolvedReconnect;
        if (!reconnect.Enabled || !_hasEstablishedSession)
        {
            return;
        }

        EnsureReconnectLoop();
    }

    async Task ReconnectLoopAsync(CancellationToken ct)
    {
        var reconnect = _config.ResolvedReconnect;
        var backoff = new ExponentialBackoff(reconnect.Backoff ?? DefaultReconnectBackoff, reconnect.MaxBackoff ?? DefaultReconnectMaxBackoff);
        var attempts = 0;

        try
        {
            while (!_closeRequested && attempts < reconnect.MaxAttempts && !_authRejected)
            {
                attempts++;
                SetState(ConnectionState.Reconnecting);

                try
                {
                    await Task.Delay(backoff.Next(), Time, ct).ConfigureAwait(false);
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
                catch (ProtocolException error) when (error.RejectionCode is not null)
                {
                    break;
                }
                catch (Exception error) when (ExceptionPolicy.IsRecoverable(error) && !_closeRequested && !_authRejected)
                {
                    // Try again after the next backoff delay.
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

    async Task RestoreReconnectStateAsync(CancellationToken ct)
    {
        var listeners = _reconnectListeners.Snapshot();
        List<Exception>? failures = null;

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
                catch (Exception exception) when (ExceptionPolicy.IsRecoverable(exception))
                {
                    _telemetry.Log(FitzLogLevel.Warn, "fitz.connection.reconnect_listener_failed", new Dictionary<string, object?>
                    {
                        ["error"] = exception.Message,
                        ["error_detail"] = exception.ToString(),
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
            _connectionClosedToken = _connectionClosedCts.Token;
        }

        CancelConnectionToken(previous);
        previous.Dispose();
    }

    void SignalConnectionClosed()
    {
        CancellationTokenSource current;
        lock (_gate)
        {
            current = _connectionClosedCts;
        }

        CancelConnectionToken(current);
    }

    void CancelConnectionToken(CancellationTokenSource source)
    {
        try
        {
            source.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            _telemetry.HandlerFailed(error);
        }
    }

    void NotifyDisconnect()
    {
        foreach (var listener in _disconnectListeners.Snapshot())
        {
            try
            {
                listener();
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
            }
        }
    }

    ITransport? DetachTransport() => Interlocked.Exchange(ref _transport, null);

    ITransport EnsureTransport() => Volatile.Read(ref _transport) ?? throw new ConnectionException("No active transport");

    (ITransport Transport, CancellationToken Session) CaptureTransportSession(CancellationToken? expectedSession)
    {
        lock (_gate)
        {
            EnsureSessionReady();
            var transport = EnsureTransport();
            var session = _connectionClosedToken;
            if (expectedSession is { } expected && (expected != session || expected.IsCancellationRequested))
                throw new ConnectionException("The owning connection session has ended.");
            return (transport, session);
        }
    }

    void EnsureSessionReady()
    {
        if (_closeRequested || (State != ConnectionState.Authenticated &&
            !(_restoreRequestDepth.Value > 0 && State == ConnectionState.Reconnecting)))
            throw new ConnectionException("The connection session changed while waiting to send.");
    }

    void EnsureAuthenticated()
    {
        if (_closeRequested || State != ConnectionState.Authenticated)
        {
            throw new ConnectionException($"Cannot use connection while state is {ConnectionTelemetry.Describe(State)}");
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
            _telemetry.RequestQueueFull();
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

        return new ConnectionException($"Cannot use connection while state is {ConnectionTelemetry.Describe(State)}");
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

    void OnMultiplexerError(Exception exception)
    {
        _telemetry.Log(FitzLogLevel.Warn, "fitz.connection.notification_dispatch_failed", new Dictionary<string, object?>
        {
            ["error"] = exception.Message,
        });
        if (exception is SubscriptionBackpressureException)
        {
            if (NotificationDeliveryFailed is { } listeners)
            {
                foreach (Action<Exception> listener in listeners.GetInvocationList())
                {
                    try
                    {
                        listener(exception);
                    }
                    catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
                    {
                    }
                }
            }
            ScheduleConnectionLoss(exception);
        }
    }

    static string DescribeConnectionLoss(Exception exception)
    {
        return exception.Message.Length > 0
            ? exception.Message
            : "connection closed during CONNECT";
    }


    static TimeSpan Min(TimeSpan left, TimeSpan right) => left <= right ? left : right;

    async Task TryCancelAsync(CancellationTokenSource? source)
    {
        if (source is null)
        {
            return;
        }

        try
        {
            await source.CancelAsync().WaitAsync(Timeout, Time, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
        }
    }

    async Task DisposeTransportAsync(ITransport transport, CancellationToken ct)
    {
        try
        {
            await transport.DisposeAsync().AsTask().WaitAsync(Timeout, Time, ct).ConfigureAwait(false);
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
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

}
