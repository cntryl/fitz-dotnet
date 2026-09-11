using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
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
public sealed class RpcClient : IRpcClient, IDisposable
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
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly Action<Exception>? _onWorkerError;
    readonly TimeSpan _responseTimeout;
    readonly Dictionary<string, Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask>> _workers = new(StringComparer.Ordinal);
    readonly Dictionary<string, uint> _workerConcurrency = new(StringComparer.Ordinal);
    readonly Dictionary<string, SemaphoreSlim> _workerGates = new(StringComparer.Ordinal);
    readonly object _workerSync = new();
    readonly object _responseSync = new();
    readonly Dictionary<Guid, RpcCallState> _calls = [];

    IDisposable? _workerReconnectRegistration;
    int _disposed;
    IDisposable? _rpcRequestRegistration;
    IDisposable? _rpcResponseRegistration;
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
            onWorkerError: connection.ReportAsyncHandlerError)
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
            onWorkerError: null)
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
        Action<Exception>? onWorkerError = null)
    {
        _request = request;
        _send = send;
        _registerNotificationHandler = registerNotificationHandler;
        _onReconnect = onReconnect;
        _getConnectionClosedToken = getConnectionClosedToken;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _onWorkerError = onWorkerError;
        _responseTimeout = connectionTimeout ?? TimeSpan.FromSeconds(30);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<RpcResponseFrame> CallAsync(
        string route,
        ReadOnlyMemory<byte> body,
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

        return CallCoreAsync(route, body, ct);
    }

    async IAsyncEnumerable<RpcResponseFrame> CallCoreAsync(
        string route,
        ReadOnlyMemory<byte> body,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var correlationId = Guid.NewGuid();
        var correlationBytes = GuidToNetworkBytes(correlationId);
        var channel = new SubscriptionChannel<RpcResponseFrame>();
        var call = new RpcCallState(channel);
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

        try
        {
            await _send(MessageTypes.RpcRequest, writer.WrittenMemory, ct).ConfigureAwait(false);

            var connectionClosedToken = _getConnectionClosedToken?.Invoke() ?? CancellationToken.None;
            using var connectionClosedRegistration = connectionClosedToken.CanBeCanceled
                ? connectionClosedToken.Register(static state => ((SubscriptionChannel<RpcResponseFrame>)state!).Dispose(), channel)
                : default;

            while (true)
            {
                SubscriptionReadResult<RpcResponseFrame> result;
                try
                {
                    result = await channel.ReadAsync(ct).AsTask().WaitAsync(_responseTimeout, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (TimeoutException)
                {
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
    }

    void CompleteRpcCall(Guid correlationId, RpcCallState call, Exception? exception = null)
    {
        lock (_responseSync)
        {
            if (!_calls.TryGetValue(correlationId, out var registeredCall) || !ReferenceEquals(registeredCall, call))
            {
                return;
            }
            _calls.Remove(correlationId);
        }
        call.Channel.Complete(exception);
    }

    /// <inheritdoc />
    public async Task<RpcWorkerRegistration> RegisterWorkerAsync(
        string pattern,
        Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> handler,
        RpcWorkerOptions? options = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsRegistrationPattern(pattern, "rpc"))
        {
            throw new RpcException($"pattern '{pattern}' must use whole-segment * or ** wildcards", "INVALID_ROUTE");
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for worker registration");
        }

        var maxConcurrency = options?.MaxConcurrency ?? 1;
        if (maxConcurrency is < 1 or > 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxConcurrency must be between 1 and 1024.");
        }

        lock (_workerSync)
        {
            if (_workers.ContainsKey(pattern))
            {
                throw new RpcException($"worker pattern '{pattern}' is already registered", "ALREADY_REGISTERED");
            }

            _workers[pattern] = handler;
            _workerConcurrency[pattern] = maxConcurrency;
            EnsureRpcRequestHandlerInitializedLocked();
            _workerGates[pattern] = new SemaphoreSlim(checked((int)maxConcurrency), checked((int)maxConcurrency));
        }

        try
        {
            await SubscribeWorkerAsync(pattern, maxConcurrency, ct).ConfigureAwait(false);
        }
        catch
        {
            RemoveWorker(pattern);
            throw;
        }

        lock (_workerSync)
        {
            _workerReconnectRegistration ??= _onReconnect?.Invoke(ResubscribeWorkersAsync);
        }

        return new RpcWorkerRegistration(pattern, unregisterToken => new ValueTask(UnsubscribeWorkerAsync(pattern, unregisterToken)));
    }

    void EnsureRpcRequestHandlerInitializedLocked()
    {
        ThrowIfDisposed();
        if (_rpcRequestHandlerInitialized || _registerNotificationHandler == null)
        {
            return;
        }

        _rpcRequestHandlerInitialized = true;
        _rpcRequestRegistration = _registerNotificationHandler(MessageTypes.RpcRequest, payload =>
        {
            if (_dispatchAsyncHandler is not null)
            {
                if (!_dispatchAsyncHandler(token => new ValueTask(HandleIncomingRequestAsync(payload, token)), null))
                {
                    _ = TrySendBackpressureResponseAsync(payload);
                }

                return;
            }

            _ = HandleIncomingRequestAsync(payload, CancellationToken.None);
        });
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "RPC worker callbacks are user code and must not break notification dispatch.")]
    async Task HandleIncomingRequestAsync(byte[] payload, CancellationToken ct)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            if (reader.RemainingBytes < CorrelationIdLength)
            {
                return;
            }

            var correlationId = reader.ReadBytes(CorrelationIdLength);
            var route = reader.ReadString();
            var bodyLength = reader.ReadU32();
            if (reader.RemainingBytes < bodyLength)
            {
                return;
            }

            var body = reader.ReadBytes(bodyLength);
            if (!reader.IsEof)
            {
                return;
            }

            if (!TryGetWorker(route, out var handler, out var concurrencyGate))
            {
                return;
            }

            var writer = new RpcResponseWriter(_send, correlationId);
            if (!await concurrencyGate.WaitAsync(0, ct).ConfigureAwait(false))
            {
                await writer.SendAsync(
                    EncodeTerminalErrorBody(RpcBackpressureErrorCode, "Local RPC worker is overloaded"),
                    isEnd: true,
                    ct).ConfigureAwait(false);
                return;
            }

            try
            {
                await handler(new RpcRequest(route, body), writer, ct).ConfigureAwait(false);
            }
            finally
            {
                concurrencyGate.Release();
            }
        }
        catch (Exception exception)
        {
            try
            {
                _onWorkerError?.Invoke(exception);
            }
            catch
            {
                // Diagnostic sinks must not tear down notification dispatch.
            }
        }
    }

    bool TryGetWorker(
        string route,
        out Func<RpcRequest, IRpcResponseWriter, CancellationToken, ValueTask> handler,
        out SemaphoreSlim concurrencyGate)
    {
        lock (_workerSync)
        {
            if (_workers.TryGetValue(route, out handler!))
            {
                concurrencyGate = _workerGates[route];
                return true;
            }

            string? bestPattern = null;
            (int LiteralSegments, int SingleWildcards, int SegmentCount) bestSpecificity = default;
            foreach (var entry in _workers)
            {
                if (RouteValidation.MatchesPattern(route, entry.Key))
                {
                    var specificity = GetPatternSpecificity(entry.Key);
                    if (bestPattern is null || specificity.CompareTo(bestSpecificity) > 0 ||
                        (specificity == bestSpecificity && string.CompareOrdinal(entry.Key, bestPattern) < 0))
                    {
                        bestPattern = entry.Key;
                        bestSpecificity = specificity;
                    }
                }
            }

            if (bestPattern is not null)
            {
                handler = _workers[bestPattern];
                concurrencyGate = _workerGates[bestPattern];
                return true;
            }
        }

        handler = default!;
        concurrencyGate = default!;
        return false;
    }

    static (int LiteralSegments, int SingleWildcards, int SegmentCount) GetPatternSpecificity(string pattern)
    {
        var pathStart = pattern.IndexOf("://", StringComparison.Ordinal) + 3;
        var segments = pattern[pathStart..].Split('/');
        var literals = 0;
        var singleWildcards = 0;
        foreach (var segment in segments)
        {
            if (segment == "*")
                singleWildcards++;
            else if (segment != "**")
                literals++;
        }
        return (literals, singleWildcards, segments.Length);
    }

    async Task SubscribeWorkerAsync(string pattern, uint maxConcurrency, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        writer.WriteU32(maxConcurrency);

        var response = await _request(MessageTypes.RpcSubscribeWorker, writer.WrittenMemory, ct).ConfigureAwait(false);
        _ = ReadRpcSuccess(response, "REGISTER");
    }

    async Task UnsubscribeWorkerAsync(string pattern, CancellationToken ct)
    {
        await UnsubscribeWorkerWireAsync(pattern, ct).ConfigureAwait(false);
        lock (_workerSync)
        {
            RemoveWorkerLocked(pattern);
            if (_workers.Count == 0)
            {
                _workerReconnectRegistration?.Dispose();
                _workerReconnectRegistration = null;
            }
        }
    }

    async Task UnsubscribeWorkerWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.RpcUnsubscribeWorker, writer.WrittenMemory, ct).ConfigureAwait(false);
        _ = ReadRpcSuccess(response, "UNREGISTER");
    }

    void RemoveWorker(string pattern)
    {
        lock (_workerSync)
        {
            RemoveWorkerLocked(pattern);
        }
    }

    void RemoveWorkerLocked(string pattern)
    {
        _workers.Remove(pattern);
        _workerConcurrency.Remove(pattern);
        _workerGates.Remove(pattern);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect restoration must best-effort roll back every already-restored worker before preserving the original failure.")]
    async ValueTask ResubscribeWorkersAsync(CancellationToken ct)
    {
        KeyValuePair<string, uint>[] snapshot;
        lock (_workerSync)
            snapshot = _workerConcurrency.ToArray();
        var restoredPatterns = new List<string>(snapshot.Length);
        try
        {
            foreach (var entry in snapshot)
            {
                await SubscribeWorkerAsync(entry.Key, entry.Value, ct).ConfigureAwait(false);
                restoredPatterns.Add(entry.Key);
            }
        }
        catch
        {
            foreach (var pattern in restoredPatterns)
            {
                try
                {
                    await UnsubscribeWorkerWireAsync(pattern, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best effort; preserve the original restore failure.
                }
            }

            throw;
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "A best-effort overload response must not break notification dispatch.")]
    async Task TrySendBackpressureResponseAsync(byte[] payload)
    {
        try
        {
            if (!TryDecodeInboundRequest(payload, out var correlationId, out _))
            {
                return;
            }

            var writer = new RpcResponseWriter(_send, correlationId);
            await writer.SendAsync(EncodeTerminalErrorBody(RpcBackpressureErrorCode, "Local RPC worker is overloaded"), isEnd: true).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Try-decode treats every malformed untrusted frame as a non-match.")]
    static bool TryDecodeInboundRequest(byte[] payload, out byte[] correlationId, out string route)
    {
        correlationId = Array.Empty<byte>();
        route = string.Empty;

        try
        {
            var reader = new BinaryBufferReader(payload);
            if (reader.RemainingBytes < CorrelationIdLength)
            {
                return false;
            }

            correlationId = reader.ReadBytes(CorrelationIdLength);
            route = reader.ReadString();
            if (reader.RemainingBytes < 4)
            {
                return false;
            }

            var bodyLength = reader.ReadU32();
            if (reader.RemainingBytes < bodyLength)
            {
                return false;
            }

            _ = reader.ReadBytes(bodyLength);
            return reader.IsEof;
        }
        catch
        {
            return false;
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
        lock (_responseSync)
        {
            _rpcResponseRegistration?.Dispose();
            _rpcResponseRegistration = null;
            calls = [.. _calls.Values];
            _calls.Clear();
        }
        foreach (var call in calls)
        {
            call.Channel.Complete(new ObjectDisposedException(nameof(RpcClient)));
        }

        lock (_workerSync)
        {
            _rpcRequestRegistration?.Dispose();
            _rpcRequestRegistration = null;
            _workerReconnectRegistration?.Dispose();
            _workerReconnectRegistration = null;
            _workers.Clear();
            _workerConcurrency.Clear();
            _workerGates.Clear();
        }
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    sealed class RpcCallState(SubscriptionChannel<RpcResponseFrame> channel)
    {
        readonly object _gate = new();
        ulong _nextSequence;

        internal SubscriptionChannel<RpcResponseFrame> Channel { get; } = channel;

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

    [SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable", Justification = "The send gate may still have concurrent holders when a worker returns and has no resource to release unless AvailableWaitHandle is used.")]
    sealed class RpcResponseWriter : IRpcResponseWriter
    {
        readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
        readonly byte[] _correlationId;
        readonly SemaphoreSlim _sendGate = new(1, 1);
        ulong _sequence;
        bool _ended;

        internal RpcResponseWriter(Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send, byte[] correlationId)
        {
            _send = send;
            _correlationId = correlationId;
        }

        public async ValueTask SendAsync(ReadOnlyMemory<byte> body, bool isEnd = false, CancellationToken ct = default)
        {
            await _sendGate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (_ended)
                {
                    throw new InvalidOperationException("The RPC response stream has already ended.");
                }

                using var writer = new BinaryBufferWriter();
                writer.WriteBytes(_correlationId);
                writer.WriteU64(_sequence);
                writer.WriteU8(isEnd ? RpcResponseFlagStreamEnd : (byte)0);
                writer.WriteU32((uint)body.Length);
                writer.WriteBytes(body.Span);

                await _send(MessageTypes.RpcResponse, writer.WrittenMemory, ct).ConfigureAwait(false);
                _sequence++;
                _ended = isEnd;
            }
            finally
            {
                _sendGate.Release();
            }
        }
    }

}
