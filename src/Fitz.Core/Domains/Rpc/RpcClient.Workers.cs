using System.Diagnostics.CodeAnalysis;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Rpc;

sealed partial class RpcClient
{
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
        EnsureRpcLifecycleHandlerInitialized();
        _rpcRequestRegistration = _registerNotificationHandler(MessageTypes.RpcRequest, payload =>
        {
            var receivedAt = System.Diagnostics.Stopwatch.GetTimestamp();
            var connectionClosed = _getConnectionClosedToken?.Invoke() ?? CancellationToken.None;
            if (!TryDecodeInboundRequest(payload, out var correlationBytes, out var route) ||
                !TryGetWorker(route, out _, out _))
            {
                return;
            }
            var queued = new QueuedRpcInvocation(payload, correlationBytes, receivedAt, connectionClosed);
            lock (_workerSync)
            {
                _queuedWorkerCalls.Add(GuidFromNetworkBytes(correlationBytes), queued);
            }
            if (_dispatchAsyncHandler is not null)
            {
                if (!_dispatchAsyncHandler(token => new ValueTask(DispatchQueuedRequestAsync(queued, token)),
                    exception => { _ = DispatchQueuedRequestAsync(queued, CancellationToken.None, reject: true); }))
                {
                    _ = DispatchQueuedRequestAsync(queued, CancellationToken.None, reject: true);
                }

                return;
            }

            _ = DispatchQueuedRequestAsync(queued, CancellationToken.None);
        });
    }

    async Task DispatchQueuedRequestAsync(QueuedRpcInvocation queued, CancellationToken ct, bool reject = false)
    {
        byte[]? payload;
        lock (_workerSync)
        {
            var invocationId = GuidFromNetworkBytes(queued.CorrelationBytes);
            if (!_queuedWorkerCalls.Remove(invocationId))
                return;
            payload = queued.Payload;
            queued.Payload = null;
            if (payload is null)
                return;
            _claimedWorkerCalls.Add(invocationId);
        }
        try
        {
            if (reject)
                await TrySendBackpressureResponseAsync(payload, queued.ConnectionClosed).ConfigureAwait(false);
            else
                await HandleIncomingRequestAsync(payload, queued.ReceivedAt, queued.ConnectionClosed, ct).ConfigureAwait(false);
        }
        finally
        {
            payload = null;
            await SendWorkerCleanupAckAsync(queued.CorrelationBytes, queued.ConnectionClosed).ConfigureAwait(false);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "RPC worker callbacks are user code and must not break notification dispatch.")]
    async Task HandleIncomingRequestAsync(byte[] payload, long receivedAt, CancellationToken connectionClosed, CancellationToken ct)
    {
        try
        {
            if (connectionClosed.IsCancellationRequested)
            {
                return;
            }
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
            TimeSpan? remainingTime = null;
            if (reader.RemainingBytes != 0)
            {
                if ((_getCapabilityBits() & ServerCapabilities.RpcCancellationBit) == 0)
                {
                    return;
                }
                if (reader.RemainingBytes != 6 || reader.ReadU8() != 1 || reader.ReadU8() != 1)
                {
                    return;
                }

                var remainingBudgetMs = reader.ReadU32();
                if (!reader.IsEof || remainingBudgetMs > 86_400_000)
                {
                    return;
                }

                var budget = TimeSpan.FromMilliseconds(remainingBudgetMs) - System.Diagnostics.Stopwatch.GetElapsedTime(receivedAt);
                remainingTime = budget > TimeSpan.Zero ? budget : TimeSpan.Zero;
            }

            if (!TryGetWorker(route, out var handler, out var concurrencyGate))
            {
                return;
            }

            var writer = new RpcResponseWriter(_send, correlationId, connectionClosed);
            if (!await concurrencyGate.WaitAsync(0, ct).ConfigureAwait(false))
            {
                await TrySendBackpressureResponseAsync(payload, connectionClosed).ConfigureAwait(false);
                return;
            }

            try
            {
                using var invocationCts = CancellationTokenSource.CreateLinkedTokenSource(
                    ct,
                    connectionClosed);
                if (remainingTime is { } budget)
                {
                    invocationCts.CancelAfter(budget);
                }
                var invocation = new ActiveRpcInvocation(invocationCts);
                var cancelImmediately = false;
                lock (_workerSync)
                {
                    var invocationId = GuidFromNetworkBytes(correlationId);
                    _claimedWorkerCalls.Remove(invocationId);
                    cancelImmediately = _cancelledWorkerCalls.Remove(invocationId);
                    _activeWorkerCalls[invocationId] = invocation;
                }
                if (cancelImmediately || remainingTime == TimeSpan.Zero)
                {
                    await invocationCts.CancelAsync().ConfigureAwait(false);
                }

                try
                {
                    await handler(new RpcRequest(route, body, remainingTime), writer, invocationCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (invocationCts.IsCancellationRequested)
                {
                }
                finally
                {
                    lock (_workerSync)
                    {
                        _activeWorkerCalls.Remove(GuidFromNetworkBytes(correlationId));
                    }
                }
            }
            catch (Exception)
            {
                if (!writer.IsEnded)
                {
                    await writer.SendAsync(
                        EncodeTerminalErrorBody(FitzErrorCodes.RpcBackendError, "Local RPC worker failed"),
                        isEnd: true,
                        ct).ConfigureAwait(false);
                }
                throw;
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
        finally
        {
            RemoveQueuedInvocation(payload);
        }
    }

    void RemoveQueuedInvocation(byte[] payload)
    {
        if (payload.Length < CorrelationIdLength)
            return;
        lock (_workerSync)
        {
            var invocationId = GuidFromNetworkBytes(payload.AsSpan(0, CorrelationIdLength));
            _queuedWorkerCalls.Remove(invocationId);
            _claimedWorkerCalls.Remove(invocationId);
            _cancelledWorkerCalls.Remove(invocationId);
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
        if ((_getCapabilityBits() & ServerCapabilities.RpcCancellationBit) != 0)
        {
            writer.WriteU8(1);
            writer.WriteU8(1);
        }

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
    async Task TrySendBackpressureResponseAsync(byte[] payload, CancellationToken connectionClosed)
    {
        try
        {
            if (!TryDecodeInboundRequest(payload, out var correlationId, out _))
            {
                return;
            }

            var writer = new RpcResponseWriter(_send, correlationId, connectionClosed);
            await writer.SendAsync(EncodeTerminalErrorBody(RpcBackpressureErrorCode, "Local RPC worker is overloaded"), isEnd: true, connectionClosed).ConfigureAwait(false);
        }
        catch
        {
        }
        finally
        {
            RemoveQueuedInvocation(payload);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Try-decode treats every malformed untrusted frame as a non-match.")]
    bool TryDecodeInboundRequest(byte[] payload, out byte[] correlationId, out string route)
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
            if (reader.IsEof)
            {
                return true;
            }

            return (_getCapabilityBits() & ServerCapabilities.RpcCancellationBit) != 0 &&
                reader.RemainingBytes == 6 && reader.ReadU8() == 1 && reader.ReadU8() == 1 &&
                reader.ReadU32() <= 86_400_000 && reader.IsEof;
        }
        catch
        {
            return false;
        }
    }

    sealed class ActiveRpcInvocation(CancellationTokenSource cancellation)
    {
        internal CancellationTokenSource Cancellation { get; } = cancellation;
    }

    sealed class QueuedRpcInvocation(byte[] payload, byte[] correlationBytes, long receivedAt, CancellationToken connectionClosed)
    {
        internal byte[]? Payload { get; set; } = payload;
        internal byte[] CorrelationBytes { get; } = correlationBytes;
        internal long ReceivedAt { get; } = receivedAt;
        internal CancellationToken ConnectionClosed { get; } = connectionClosed;
    }

    [SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable", Justification = "The send gate may still have concurrent holders when a worker returns and has no resource to release unless AvailableWaitHandle is used.")]
    sealed class RpcResponseWriter : IRpcResponseWriter
    {
        readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
        readonly byte[] _correlationId;
        readonly CancellationToken _connectionClosed;
        readonly SemaphoreSlim _sendGate = new(1, 1);
        ulong _sequence;
        bool _ended;

        internal bool IsEnded => _ended;

        internal RpcResponseWriter(Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send, byte[] correlationId,
            CancellationToken connectionClosed)
        {
            _send = send;
            _correlationId = correlationId;
            _connectionClosed = connectionClosed;
        }

        public async ValueTask SendAsync(ReadOnlyMemory<byte> body, bool isEnd = false, CancellationToken ct = default)
        {
            using var linked = _connectionClosed.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(ct, _connectionClosed) : null;
            var sendToken = linked?.Token ?? ct;
            await _sendGate.WaitAsync(sendToken).ConfigureAwait(false);
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

                await _send(MessageTypes.RpcResponse, writer.WrittenMemory, sendToken).ConfigureAwait(false);
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
