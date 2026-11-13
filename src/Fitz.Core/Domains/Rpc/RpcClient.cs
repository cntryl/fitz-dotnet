using System.Runtime.CompilerServices;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Rpc;

/// <summary>
/// The default <see cref="IRpcClient"/>: RPC calls and worker registrations.
/// </summary>
/// <remarks>
/// This class owns the caller side (calls, response streams, cancellation) and routes the
/// worker side to <see cref="RpcWorkerHost"/>; <see cref="RpcWire"/> holds the wire format.
/// Obtained from <see cref="FitzClient"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class RpcClient : IRpcClient, IDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<CancellationToken>> _sendInSession;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    readonly Func<CancellationToken>? _getConnectionClosedToken;
    readonly RpcWorkerHost? _workers;
    static readonly TimeSpan DefaultResponseTimeout = TimeSpan.FromSeconds(30);

    // How long a cancellation waits for the broker's acknowledgement before it is reported as unconfirmed.
    static readonly TimeSpan CancellationConfirmationWindow = TimeSpan.FromSeconds(5);

    readonly TimeSpan _responseTimeout;
    readonly TimeProvider _time;
    readonly Lock _responseSync = new();
    readonly Dictionary<Guid, RpcCallState> _calls = [];
    readonly Dictionary<Guid, PendingCancellation> _pendingCancellations = [];

    bool _disposed;
    IDisposable? _rpcResponseRegistration;
    IDisposable? _rpcLifecycleRegistration;

    internal RpcClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.SendAsync,
            connection.RegisterNotificationHandler,
            connection.OnReconnect,
            () => connection.ConnectionClosedToken,
            (handler, rejected) => connection.TryDispatchAsyncHandler("rpc", handler, rejected),
            connectionTimeout: connection.Timeout,
            onWorkerError: connection.ReportAsyncHandlerError,
            sendInSession: connection.SendInSessionAsync,
            timeProvider: connection.Time)
    {
    }

    /// <summary>
    /// Creates a domain client over a request delegate, for testing without a broker.
    /// </summary>
    public RpcClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, byte[], CancellationToken, Task>? send = null,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, IDisposable>? onReconnect = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null,
        TimeSpan? connectionTimeout = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            AdaptSend(request, send),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler),
            onReconnect,
            getConnectionClosedToken,
            dispatchAsyncHandler is null
                ? null
                : (handler, _) => dispatchAsyncHandler(handler),
            connectionTimeout,
            onWorkerError: null)
    {
        ArgumentNullException.ThrowIfNull(request);
    }

    // Without a send delegate, fire-and-forget frames go through the request delegate.
    static Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> AdaptSend(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, byte[], CancellationToken, Task>? send)
    {
        if (send is null)
            return async (messageType, payload, ct) => _ = await request(messageType, payload.ToArray(), ct).ConfigureAwait(false);

        return async (messageType, payload, ct) => await send(messageType, payload.ToArray(), ct).ConfigureAwait(false);
    }

    internal RpcClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, IDisposable>? onReconnect = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        TimeSpan? connectionTimeout = null,
        Action<Exception>? onWorkerError = null,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<CancellationToken>>? sendInSession = null,
        TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _send = send;
        _sendInSession = sendInSession ?? SendWithCapturedSessionAsync;
        _registerNotificationHandler = registerNotificationHandler;
        _getConnectionClosedToken = getConnectionClosedToken;
        _responseTimeout = connectionTimeout ?? DefaultResponseTimeout;
        _workers = registerNotificationHandler is null
            ? null
            : new RpcWorkerHost(
                this,
                request,
                send,
                registerNotificationHandler,
                EnsureRpcLifecycleHandlerInitialized,
                onReconnect,
                getConnectionClosedToken,
                dispatchAsyncHandler,
                onWorkerError,
                _time);
    }

    async ValueTask<CancellationToken> SendWithCapturedSessionAsync(
        ushort messageType,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct)
    {
        // Delegate-backed test clients have no transport-generation API. Capture their
        // supplied lifetime before sending; real connections return the actual lifetime.
        var session = _getConnectionClosedToken?.Invoke() ?? CancellationToken.None;
        await _send(messageType, payload, ct).ConfigureAwait(false);
        return session;
    }

    /// <inheritdoc />
    public RpcCall CallAsync(
        string route,
        ReadOnlyMemory<byte> body,
        CancellationToken ct = default) => CallAsync(route, body, null, ct);

    /// <inheritdoc />
    public RpcCall CallAsync(
        string route,
        ReadOnlyMemory<byte> body,
        TimeSpan? timeout,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsConcreteRoute(route, "rpc"))
        {
            throw new ArgumentException($"route '{route}' must be a concrete rpc route", nameof(route));
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for RPC streaming");
        }

        if (timeout is { } requestedTimeout && requestedTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "RPC timeout cannot be negative.");
        }
        if (timeout is { } boundedTimeout && boundedTimeout > RpcWire.MaxCallBudget)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "RPC timeout cannot exceed one day.");
        }

        var cancellation = new TaskCompletionSource<RpcCancellationOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        return new RpcCall(CallCoreAsync(route, body, timeout, cancellation, ct), cancellation.Task);
    }

    async IAsyncEnumerable<RpcResponseFrame> CallCoreAsync(
        string route,
        ReadOnlyMemory<byte> body,
        TimeSpan? timeout,
        TaskCompletionSource<RpcCancellationOutcome> cancellation,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var budgetStartedAt = _time.GetTimestamp();
        using var timeoutCts = timeout is { } duration ? new CancellationTokenSource(duration, _time) : null;
        using var linkedCts = timeoutCts is not null
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token)
            : null;
        var callToken = linkedCts?.Token ?? ct;
        // An explicit budget bounds the whole call, so a slow first frame within it is not an
        // error. Without one, each frame must arrive within the connection's response timeout.
        var frameTimeout = timeout is null ? _responseTimeout : Timeout.InfiniteTimeSpan;
        var correlationId = Guid.NewGuid();
        var correlationBytes = RpcWire.GuidToNetworkBytes(correlationId);
        var channel = new SubscriptionChannel<RpcResponseFrame>();
        var call = new RpcCallState(channel, cancellation);
        lock (_responseSync)
        {
            EnsureRpcResponseHandlerInitializedLocked();
            _calls.Add(correlationId, call);
        }

        using var writer = RpcWire.EncodeRequest(
            correlationBytes,
            route,
            body.Span,
            timeout - _time.GetElapsedTime(budgetStartedAt));

        var requestSent = false;
        var connectionClosedToken = CancellationToken.None;
        byte cancellationReason = 1;
        try
        {
            callToken.ThrowIfCancellationRequested();
            connectionClosedToken = await _sendInSession(MessageTypes.RpcRequest, writer.WrittenMemory, callToken).ConfigureAwait(false);
            requestSent = true;

            using var connectionClosedRegistration = connectionClosedToken.CanBeCanceled
                ? connectionClosedToken.Register(static state => ((SubscriptionChannel<RpcResponseFrame>)state!).Dispose(), channel)
                : default;

            while (true)
            {
                SubscriptionReadResult<RpcResponseFrame> result;
                try
                {
                    result = await channel.ReadAsync(callToken).AsTask().WaitAsync(frameTimeout, _time, callToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (callToken.IsCancellationRequested)
                {
                    cancellationReason = timeoutCts?.IsCancellationRequested == true ? (byte)2 : (byte)1;
                    throw;
                }
                catch (TimeoutException)
                {
                    cancellationReason = 2;
                    throw new RequestTimeoutException($"RPC stream timed out after {frameTimeout.TotalMilliseconds}ms without a response frame");
                }

                if (connectionClosedToken.IsCancellationRequested)
                {
                    throw new ConnectionException("Connection closed or reset");
                }

                if (!result.HasItem)
                {
                    break;
                }

                yield return result.Item;
            }
        }
        finally
        {
            lock (_responseSync)
            {
                _calls.Remove(correlationId);
            }
            channel.Dispose();
            if (call.IsTerminal)
            {
                cancellation.TrySetResult(RpcCancellationOutcome.NotRequested);
            }
            else if (!requestSent)
            {
                cancellation.TrySetResult(RpcCancellationOutcome.RequestNotSent);
            }
            else if (connectionClosedToken.IsCancellationRequested)
            {
                cancellation.TrySetResult(RpcCancellationOutcome.ConnectionClosed);
            }
            else
            {
                await RequestCancellationAsync(correlationId, correlationBytes, call, cancellationReason).ConfigureAwait(false);
            }
        }
    }

    void HandleRpcResponse(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            if (reader.RemainingBytes < RpcWire.CorrelationIdLength)
            {
                return;
            }

            var correlationId = RpcWire.GuidFromNetworkBytes(reader.ReadSpan(RpcWire.CorrelationIdLength));
            RpcCallState? call;
            lock (_responseSync)
            {
                _calls.TryGetValue(correlationId, out call);
            }
            if (call is null)
            {
                return;
            }

            var sequence = reader.ReadU64();
            var flags = reader.ReadU8();
            if ((flags & ~RpcWire.ResponseFlagStreamEnd) != 0)
            {
                throw new ProtocolException("RPC response contains unsupported flags.");
            }

            var responseBody = reader.ReadLengthPrefixed();
            if (!reader.IsEof)
            {
                throw new ProtocolException("RPC response contains trailing bytes.");
            }

            var streamEnd = (flags & RpcWire.ResponseFlagStreamEnd) != 0;
            if (streamEnd && RpcWire.TryDecodeTerminalError(responseBody) is { } rpcError)
            {
                CompleteRpcCall(correlationId, call, rpcError);
                return;
            }

            if (!call.TryAcceptSequence(sequence))
            {
                CompleteRpcCall(correlationId, call,
                    new RpcException($"RPC response sequence {sequence} is out of order", "INVALID_SEQUENCE", domainCode: FitzErrorCodes.RpcInvalidSequence));
                return;
            }

            if (!streamEnd || responseBody.Length > 0)
            {
                call.Channel.PostNotification(new RpcResponseFrame(responseBody.AsMemory(), sequence));
            }

            if (streamEnd)
            {
                CompleteRpcCall(correlationId, call);
            }
        }
        catch (FitzException exception) when (RpcWire.PeekCorrelationId(payload) is { } failedId)
        {
            RpcCallState? call;
            lock (_responseSync)
            {
                _calls.TryGetValue(failedId, out call);
            }
            if (call is not null)
            {
                CompleteRpcCall(failedId, call, exception);
            }
        }
    }

    void EnsureRpcResponseHandlerInitializedLocked()
    {
        ThrowIfDisposed();
        _rpcResponseRegistration ??= _registerNotificationHandler!(MessageTypes.RpcResponse, HandleRpcResponse);
        EnsureRpcLifecycleHandlerInitializedLocked();
    }

    void EnsureRpcLifecycleHandlerInitializedLocked() =>
        _rpcLifecycleRegistration ??= _registerNotificationHandler!(MessageTypes.RpcLifecycle, HandleRpcLifecycle);

    void CompleteRpcCall(Guid correlationId, RpcCallState call, Exception? exception = null)
    {
        lock (_responseSync)
        {
            if (!_calls.TryGetValue(correlationId, out var registeredCall) || !ReferenceEquals(registeredCall, call))
            {
                return;
            }
            call.MarkTerminal();
            _calls.Remove(correlationId);
        }
        call.Channel.Complete(exception);
        call.Cancellation.TrySetResult(RpcCancellationOutcome.NotRequested);
    }

    async Task RequestCancellationAsync(
        Guid correlationId,
        byte[] correlationBytes,
        RpcCallState call,
        byte reason)
    {
        var pending = new PendingCancellation(call.Cancellation);
        lock (_responseSync)
        {
            if (_pendingCancellations.ContainsKey(correlationId))
            {
                return;
            }
            _pendingCancellations.Add(correlationId, pending);
        }
        _ = ExpireCancellationAsync(correlationId, pending);

        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteBytes(correlationBytes);
        writer.WriteU8(reason);
        try
        {
            await _send(MessageTypes.RpcCancel, writer.WrittenMemory, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception) when (ExceptionPolicy.IsRecoverable(exception))
        {
            CompleteCancellation(
                correlationId,
                pending,
                exception is ConnectionException or ObjectDisposedException
                    ? RpcCancellationOutcome.ConnectionClosed
                    : RpcCancellationOutcome.RequestNotSent);
        }
    }

    async Task ExpireCancellationAsync(Guid correlationId, PendingCancellation pending)
    {
        await Task.Delay(CancellationConfirmationWindow, _time).ConfigureAwait(false);
        CompleteCancellation(correlationId, pending, RpcCancellationOutcome.Unconfirmed);
    }

    void CompleteCancellation(
        Guid correlationId,
        PendingCancellation pending,
        RpcCancellationOutcome outcome)
    {
        lock (_responseSync)
        {
            if (!_pendingCancellations.TryGetValue(correlationId, out var registered) || !ReferenceEquals(registered, pending))
            {
                return;
            }
            _pendingCancellations.Remove(correlationId);
        }
        pending.Result.TrySetResult(outcome);
    }

    void HandleRpcLifecycle(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != 18)
        {
            return;
        }

        var correlationId = RpcWire.GuidFromNetworkBytes(payload.Span.Slice(1, RpcWire.CorrelationIdLength));
        if (payload.Span[0] == 2 && payload.Span[17] is >= 1 and <= 4)
        {
            _workers?.Cancel(correlationId);
            return;
        }

        if (payload.Span[0] != 4 || RpcWire.MapCancellationStatus(payload.Span[17]) is not { } outcome)
        {
            return;
        }

        PendingCancellation? pending;
        lock (_responseSync)
        {
            _pendingCancellations.TryGetValue(correlationId, out pending);
        }
        if (pending is not null)
        {
            CompleteCancellation(correlationId, pending, outcome);
        }
    }

    void EnsureRpcLifecycleHandlerInitialized()
    {
        if (_registerNotificationHandler is null)
        {
            return;
        }
        lock (_responseSync)
        {
            ThrowIfDisposed();
            EnsureRpcLifecycleHandlerInitializedLocked();
        }
    }

    /// <inheritdoc />
    public Task<RpcWorkerRegistration> RegisterWorkerAsync(
        string pattern,
        Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> handler,
        RpcWorkerOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsRegistrationPattern(pattern, "rpc"))
        {
            throw new ArgumentException($"pattern '{pattern}' must use whole-segment * or ** wildcards", nameof(pattern));
        }

        if (_workers is null)
        {
            throw new InvalidOperationException("Notification handlers not configured for worker registration");
        }

        var maxConcurrency = options?.MaxConcurrency ?? 1;
        if (maxConcurrency is < 1 or > 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxConcurrency must be between 1 and 1024.");
        }

        return _workers.RegisterAsync(pattern, handler, maxConcurrency, ct);
    }

    /// <summary>Releases local resources and ends any registrations this client owns.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, true))
        {
            return;
        }

        RpcCallState[] calls;
        PendingCancellation[] pendingCancellations;
        lock (_responseSync)
        {
            _rpcResponseRegistration?.Dispose();
            _rpcResponseRegistration = null;
            _rpcLifecycleRegistration?.Dispose();
            _rpcLifecycleRegistration = null;
            calls = [.. _calls.Values];
            _calls.Clear();
            pendingCancellations = [.. _pendingCancellations.Values];
            _pendingCancellations.Clear();
        }
        foreach (var call in calls)
        {
            call.Channel.Complete(new ObjectDisposedException(nameof(RpcClient)));
            call.Cancellation.TrySetResult(RpcCancellationOutcome.ConnectionClosed);
        }
        foreach (var pending in pendingCancellations)
        {
            pending.Result.TrySetResult(RpcCancellationOutcome.ConnectionClosed);
        }

        _workers?.Dispose();
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);

    sealed class RpcCallState(
        SubscriptionChannel<RpcResponseFrame> channel,
        TaskCompletionSource<RpcCancellationOutcome> cancellation)
    {
        readonly Lock _gate = new();
        ulong _nextSequence;
        bool _terminal;

        internal SubscriptionChannel<RpcResponseFrame> Channel { get; } = channel;
        internal TaskCompletionSource<RpcCancellationOutcome> Cancellation { get; } = cancellation;
        internal bool IsTerminal
        {
            get
            {
                lock (_gate)
                {
                    return _terminal;
                }
            }
        }

        internal void MarkTerminal()
        {
            lock (_gate)
            {
                _terminal = true;
            }
        }

        internal bool TryAcceptSequence(ulong sequence)
        {
            lock (_gate)
            {
                if (sequence != _nextSequence)
                {
                    return false;
                }

                _nextSequence++;
                return true;
            }
        }
    }

    sealed class PendingCancellation(TaskCompletionSource<RpcCancellationOutcome> result)
    {
        internal TaskCompletionSource<RpcCancellationOutcome> Result { get; } = result;
    }
}
