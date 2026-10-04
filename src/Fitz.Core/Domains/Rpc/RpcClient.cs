using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Rpc;

/// <summary>
/// The default <see cref="IRpcClient"/>: RPC calls and worker registrations.
/// </summary>
/// <remarks>
/// Obtained from <see cref="Client"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed partial class RpcClient : IRpcClient, IDisposable
{
    const int CorrelationIdLength = 16;
    const byte RpcResponseFlagStreamEnd = 0x01;
    const uint RpcErrorCodeMin = 6001;
    const uint RpcErrorCodeMax = 6013;
    const uint RpcBackpressureErrorCode = FitzErrorCodes.RpcBackpressure;

    static byte[] GuidToNetworkBytes(Guid value)
    {
        var bytes = value.ToByteArray();
        var a = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(0, 4));
        var b = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(4, 2));
        var c = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(6, 2));
        BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(0, 4), a);
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(4, 2), b);
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(6, 2), c);
        return bytes;
    }

    static Guid GuidFromNetworkBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != CorrelationIdLength)
        {
            throw new ProtocolException("RPC correlation UUID must contain 16 bytes.");
        }
        Span<byte> little = stackalloc byte[CorrelationIdLength];
        bytes.CopyTo(little);
        BinaryPrimitives.WriteUInt32LittleEndian(little[..4], BinaryPrimitives.ReadUInt32BigEndian(bytes[..4]));
        BinaryPrimitives.WriteUInt16LittleEndian(little.Slice(4, 2), BinaryPrimitives.ReadUInt16BigEndian(bytes.Slice(4, 2)));
        BinaryPrimitives.WriteUInt16LittleEndian(little.Slice(6, 2), BinaryPrimitives.ReadUInt16BigEndian(bytes.Slice(6, 2)));
        return new Guid(little);
    }

    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;
    readonly Func<Func<CancellationToken, ValueTask>, IDisposable>? _onReconnect;
    readonly Func<CancellationToken>? _getConnectionClosedToken;
    readonly Func<uint> _getCapabilityBits;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly Action<Exception>? _onWorkerError;
    readonly TimeSpan _responseTimeout;
    readonly Dictionary<string, Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask>> _workers = new(StringComparer.Ordinal);
    readonly Dictionary<string, uint> _workerConcurrency = new(StringComparer.Ordinal);
    readonly Dictionary<string, SemaphoreSlim> _workerGates = new(StringComparer.Ordinal);
    readonly object _workerSync = new();
    readonly Dictionary<Guid, ActiveRpcInvocation> _activeWorkerCalls = [];
    readonly HashSet<Guid> _cancelledWorkerCalls = [];
    readonly HashSet<Guid> _queuedWorkerCalls = [];
    readonly object _responseSync = new();
    readonly Dictionary<Guid, RpcCallState> _calls = [];
    readonly Dictionary<Guid, PendingCancellation> _pendingCancellations = [];

    IDisposable? _workerReconnectRegistration;
    int _disposed;
    IDisposable? _rpcRequestRegistration;
    IDisposable? _rpcResponseRegistration;
    IDisposable? _rpcLifecycleRegistration;
    bool _rpcRequestHandlerInitialized;

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
            getCapabilityBits: () => connection.Capabilities.CapabilityBits)
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
        TimeSpan? connectionTimeout = null,
        Func<uint>? getCapabilityBits = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            send is null
                ? async (messageType, payload, ct) => { _ = await request(messageType, payload.ToArray(), ct).ConfigureAwait(false); }
    : async (messageType, payload, ct) => await send(messageType, payload.ToArray(), ct).ConfigureAwait(false),
            registerNotificationHandler,
            onReconnect,
            getConnectionClosedToken,
            dispatchAsyncHandler is null
                ? null
                : (handler, _) => dispatchAsyncHandler(handler),
            connectionTimeout,
            onWorkerError: null,
            getCapabilityBits: getCapabilityBits)
    {
        ArgumentNullException.ThrowIfNull(request);
    }

    internal RpcClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, IDisposable>? onReconnect = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        TimeSpan? connectionTimeout = null,
        Action<Exception>? onWorkerError = null,
        Func<uint>? getCapabilityBits = null)
    {
        _request = request;
        _send = send;
        _registerNotificationHandler = registerNotificationHandler;
        _onReconnect = onReconnect;
        _getConnectionClosedToken = getConnectionClosedToken;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _onWorkerError = onWorkerError;
        _getCapabilityBits = getCapabilityBits ?? (() => 0);
        _responseTimeout = connectionTimeout ?? TimeSpan.FromSeconds(30);
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
            throw new RpcException($"route '{route}' must be a concrete rpc route", "INVALID_ROUTE");
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for RPC streaming");
        }

        if (timeout is { } requestedTimeout && requestedTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "RPC timeout cannot be negative.");
        }
        if (timeout is { } boundedTimeout && boundedTimeout > TimeSpan.FromMilliseconds(86_400_000))
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
        var budgetStartedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        using var timeoutCts = timeout is { } duration ? new CancellationTokenSource(duration) : null;
        using var linkedCts = timeoutCts is not null
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token)
            : null;
        var callToken = linkedCts?.Token ?? ct;
        var correlationId = Guid.NewGuid();
        var correlationBytes = GuidToNetworkBytes(correlationId);
        var channel = new SubscriptionChannel<RpcResponseFrame>();
        var call = new RpcCallState(channel, cancellation);
        lock (_responseSync)
        {
            EnsureRpcResponseHandlerInitializedLocked();
            _calls.Add(correlationId, call);
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteBytes(correlationBytes);
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);
        if (timeout is not null && (_getCapabilityBits() & ServerCapabilities.RpcCancellationBit) != 0)
        {
            var remaining = timeout.Value - System.Diagnostics.Stopwatch.GetElapsedTime(budgetStartedAt);
            var remainingMs = (uint)Math.Clamp((long)remaining.TotalMilliseconds, 0L, 86_400_000L);
            writer.WriteU8(1);
            writer.WriteU8(1);
            writer.WriteU32(remainingMs);
        }

        var requestSent = false;
        byte cancellationReason = 1;
        try
        {
            callToken.ThrowIfCancellationRequested();
            await _send(MessageTypes.RpcRequest, writer.WrittenMemory, callToken).ConfigureAwait(false);
            requestSent = true;

            var connectionClosedToken = _getConnectionClosedToken?.Invoke() ?? CancellationToken.None;
            using var connectionClosedRegistration = connectionClosedToken.CanBeCanceled
                ? connectionClosedToken.Register(static state => ((SubscriptionChannel<RpcResponseFrame>)state!).Dispose(), channel)
                : default;

            while (true)
            {
                SubscriptionReadResult<RpcResponseFrame> result;
                try
                {
                    result = await channel.ReadAsync(callToken).AsTask().WaitAsync(_responseTimeout, callToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (callToken.IsCancellationRequested)
                {
                    cancellationReason = timeoutCts?.IsCancellationRequested == true ? (byte)2 : (byte)1;
                    throw;
                }
                catch (TimeoutException)
                {
                    cancellationReason = 2;
                    throw new RequestTimeoutException($"RPC stream timed out after {_responseTimeout.TotalMilliseconds}ms");
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
            else
            {
                await RequestCancellationAsync(correlationId, correlationBytes, call, cancellationReason).ConfigureAwait(false);
            }
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed RPC responses fail only their correlated call.")]
    void HandleRpcResponse(byte[] payload)
    {
        Guid correlationId;
        try
        {
            var reader = new BinaryBufferReader(payload);
            if (reader.RemainingBytes < CorrelationIdLength)
            {
                return;
            }

            correlationId = GuidFromNetworkBytes(reader.ReadSpan(CorrelationIdLength));
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
            if ((flags & ~RpcResponseFlagStreamEnd) != 0)
            {
                throw new ProtocolException("RPC response contains unsupported flags.");
            }

            var responseBody = reader.ReadBytes(reader.ReadU32());
            if (!reader.IsEof)
            {
                throw new ProtocolException("RPC response contains trailing bytes.");
            }

            var streamEnd = (flags & RpcResponseFlagStreamEnd) != 0;
            if (streamEnd && TryDecodeTerminalError(responseBody, out var rpcError))
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
        catch (Exception exception)
        {
            if (payload.Length < CorrelationIdLength)
            {
                return;
            }

            correlationId = GuidFromNetworkBytes(payload.AsSpan(0, CorrelationIdLength));
            RpcCallState? call;
            lock (_responseSync)
            {
                _calls.TryGetValue(correlationId, out call);
            }
            if (call is not null)
            {
                CompleteRpcCall(correlationId, call, exception);
            }
        }
    }

    void EnsureRpcResponseHandlerInitializedLocked()
    {
        ThrowIfDisposed();
        _rpcResponseRegistration ??= _registerNotificationHandler!(MessageTypes.RpcResponse, HandleRpcResponse);
        EnsureRpcLifecycleHandlerInitializedLocked();
    }

    void EnsureRpcLifecycleHandlerInitializedLocked()
    {
        if ((_getCapabilityBits() & ServerCapabilities.RpcCancellationBit) == 0)
        {
            return;
        }
        _rpcLifecycleRegistration ??= _registerNotificationHandler!(MessageTypes.RpcLifecycle, HandleRpcLifecycle);
    }

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

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Cancellation is best-effort and its status is returned on the call handle.")]
    async Task RequestCancellationAsync(
        Guid correlationId,
        byte[] correlationBytes,
        RpcCallState call,
        byte reason)
    {
        if ((_getCapabilityBits() & ServerCapabilities.RpcCancellationBit) == 0)
        {
            call.Cancellation.TrySetResult(RpcCancellationOutcome.Unsupported);
            return;
        }

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
        catch (Exception exception)
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
        await Task.Delay(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
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

    void HandleRpcLifecycle(byte[] payload)
    {
        if (payload.Length != 18)
        {
            return;
        }

        var correlationId = GuidFromNetworkBytes(payload.AsSpan(1, CorrelationIdLength));
        if (payload[0] == 2 && payload[17] is >= 1 and <= 4)
        {
            CancellationTokenSource? cancellation = null;
            lock (_workerSync)
            {
                if (_activeWorkerCalls.TryGetValue(correlationId, out var invocation))
                {
                    cancellation = invocation.Cancellation;
                }
                else if (_queuedWorkerCalls.Contains(correlationId))
                {
                    _cancelledWorkerCalls.Add(correlationId);
                }
            }
            CancelWorkerInvocation(cancellation);
            return;
        }

        if (payload[0] != 4 || MapCancellationStatus(payload[17]) is not { } outcome)
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

    static RpcCancellationOutcome? MapCancellationStatus(byte status) => status switch
    {
        1 => RpcCancellationOutcome.QueuedRemoved,
        2 => RpcCancellationOutcome.Forwarded,
        3 => RpcCancellationOutcome.WorkerUnsupported,
        4 => RpcCancellationOutcome.AlreadyTerminal,
        5 => RpcCancellationOutcome.UnknownOrUnauthorized,
        6 => RpcCancellationOutcome.ForwardingFailed,
        _ => null,
    };

    void CancelWorkerInvocation(CancellationTokenSource? cancellation)
    {
        if (cancellation is null)
        {
            return;
        }
        _ = CancelWorkerInvocationAsync(cancellation);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "User cancellation callbacks must not escape notification dispatch.")]
    async Task CancelWorkerInvocationAsync(CancellationTokenSource cancellation)
    {
        try
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception exception)
        {
            try
            {
                _onWorkerError?.Invoke(exception);
            }
            catch
            {
            }
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Worker cleanup acknowledgments are best-effort lifecycle messages.")]
    async Task SendWorkerCleanupAckAsync(byte[] correlationBytes, CancellationToken connectionClosed)
    {
        if (connectionClosed.IsCancellationRequested || (_getCapabilityBits() & ServerCapabilities.RpcCancellationBit) == 0)
        {
            return;
        }
        using var ack = new BinaryBufferWriter();
        ack.WriteU8(3);
        ack.WriteBytes(correlationBytes);
        try
        {
            await _send(MessageTypes.RpcCancel, ack.WrittenMemory, connectionClosed).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            try
            {
                _onWorkerError?.Invoke(exception);
            }
            catch
            {
            }
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

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Try-decode treats every malformed untrusted frame as a non-match.")]
    static bool TryDecodeTerminalError(ReadOnlySpan<byte> payload, out RpcException rpcError)
    {
        rpcError = null!;

        try
        {
            if (payload.Length < 5)
            {
                return false;
            }

            var reader = new BinaryBufferReader(payload.ToArray());
            if (reader.ReadU8() != 1)
            {
                return false;
            }

            var code = reader.ReadU32();
            if (code < RpcErrorCodeMin || code > RpcErrorCodeMax)
            {
                return false;
            }

            var message = reader.ReadString();
            if (!reader.IsEof)
            {
                return false;
            }

            rpcError = new RpcException(
                string.IsNullOrWhiteSpace(message) ? "RPC error" : message,
                MapRpcErrorCode(code),
                1,
                code);
            return true;
        }
        catch
        {
            return false;
        }
    }

    static byte[] EncodeTerminalErrorBody(uint code, string message)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU32(code);
        writer.WriteString(message);
        return writer.Build();
    }

    static string MapRpcErrorCode(uint code)
    {
        return code switch
        {
            FitzErrorCodes.RpcTimeout => "TIMEOUT",
            FitzErrorCodes.RpcWorkerNotFound => "WORKER_NOT_FOUND",
            FitzErrorCodes.RpcBackpressure => "BACKPRESSURE",
            FitzErrorCodes.RpcRouteNotRegistered => "ROUTE_NOT_REGISTERED",
            FitzErrorCodes.RpcCorrelationNotFound => "CORRELATION_NOT_FOUND",
            FitzErrorCodes.RpcInvalidSequence => "INVALID_SEQUENCE",
            FitzErrorCodes.RpcDuplicateCorrelation => "DUPLICATE_CORRELATION",
            FitzErrorCodes.RpcWrongWorker => "WRONG_WORKER",
            FitzErrorCodes.RpcUnauthorized => "UNAUTHORIZED",
            FitzErrorCodes.RpcBackendError => "BACKEND_ERROR",
            FitzErrorCodes.RpcInvalidRoute => "INVALID_ROUTE",
            FitzErrorCodes.RpcInvalidSubscriptionPattern => "INVALID_SUBSCRIPTION_PATTERN",
            FitzErrorCodes.RpcSubscriptionLimit => "SUBSCRIPTION_LIMIT",
            _ => "DOMAIN_ERROR",
        };
    }

    static byte[] ReadRpcSuccess(ReadOnlyMemory<byte> response, string operation)
    {
        if (response.IsEmpty)
        {
            throw new RpcException($"{operation} response is empty", $"{operation}_INVALID_RESPONSE");
        }

        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status == 0)
        {
            var data = reader.ReadBytes(reader.ReadU32());
            if (!reader.IsEof)
            {
                throw new RpcException($"{operation} success response has trailing bytes", $"{operation}_INVALID_RESPONSE");
            }
            return data;
        }

        if (status != 1)
        {
            throw new RpcException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        var domainCode = reader.ReadU32();
        var message = reader.ReadString();
        if (!reader.IsEof)
        {
            throw new RpcException($"{operation} error response has trailing bytes", $"{operation}_INVALID_RESPONSE");
        }

        throw new RpcException($"{operation} failed: {message}", MapRpcErrorCode(domainCode), status, domainCode);
    }

    /// <summary>Releases local resources and ends any registrations this client owns.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
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

        CancellationTokenSource[] activeWorkerCalls;
        lock (_workerSync)
        {
            _rpcRequestRegistration?.Dispose();
            _rpcRequestRegistration = null;
            _workerReconnectRegistration?.Dispose();
            _workerReconnectRegistration = null;
            _workers.Clear();
            _workerConcurrency.Clear();
            _workerGates.Clear();
            activeWorkerCalls = [.. _activeWorkerCalls.Values.Select(static invocation => invocation.Cancellation)];
            _activeWorkerCalls.Clear();
            _cancelledWorkerCalls.Clear();
            _queuedWorkerCalls.Clear();
        }
        foreach (var cancellation in activeWorkerCalls)
        {
            CancelWorkerInvocation(cancellation);
        }
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    sealed class RpcCallState(
        SubscriptionChannel<RpcResponseFrame> channel,
        TaskCompletionSource<RpcCancellationOutcome> cancellation)
    {
        readonly object _gate = new();
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
