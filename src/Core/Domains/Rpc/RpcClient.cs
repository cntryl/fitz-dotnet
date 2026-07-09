using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Rpc;

public sealed class RpcClient : IRpcClient
{
    private const int CorrelationIdLength = 16;
    private const byte RpcResponseFlagStreamEnd = 0x01;
    private const uint RpcErrorCodeMin = 6001;
    private const uint RpcErrorCodeMax = 6010;
    private const int DefaultWorkerMaxConcurrency = 1;
    private const uint RpcBackpressureErrorCode = 6003;

    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;
    private readonly Func<Func<CancellationToken, ValueTask>, IDisposable>? _onReconnect;
    private readonly Func<CancellationToken>? _getConnectionClosedToken;
    private readonly Func<Func<CancellationToken, ValueTask>, bool>? _dispatchAsyncHandler;
    private readonly TimeSpan _responseTimeout;
    private readonly Dictionary<string, Func<RpcRequest, IRpcResponseWriter, CancellationToken, Task>> _workers = new(StringComparer.Ordinal);

    private IDisposable? _workerReconnectRegistration;
    private bool _rpcRequestHandlerInitialized;

    internal RpcClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.SendAsync,
            connection.RegisterNotificationHandler,
            connection.OnReconnect,
            () => connection.ConnectionClosedToken,
            connection.TryDispatchAsyncHandler,
            connectionTimeout: connection.Timeout)
    {
    }

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
            dispatchAsyncHandler,
            connectionTimeout)
    {
    }

    internal RpcClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, IDisposable>? onReconnect = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null,
        TimeSpan? connectionTimeout = null)
    {
        _request = request;
        _send = send;
        _registerNotificationHandler = registerNotificationHandler;
        _onReconnect = onReconnect;
        _getConnectionClosedToken = getConnectionClosedToken;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _responseTimeout = connectionTimeout ?? TimeSpan.FromSeconds(30);
    }

    public async IAsyncEnumerable<RpcResponseFrame> CallAsync(
        string route,
        ReadOnlyMemory<byte> body,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!RouteValidation.IsConcreteRoute(route, "rpc"))
        {
            throw new RpcException($"route '{route}' must be a concrete rpc route", "INVALID_ROUTE");
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for RPC streaming");
        }

        var correlationId = GC.AllocateUninitializedArray<byte>(16);
        RandomNumberGenerator.Fill(correlationId);

        var channel = new SubscriptionChannel<RpcResponseFrame>();
        ExceptionDispatchInfo? terminalError = null;
        IDisposable? registration = null;
        registration = _registerNotificationHandler(MessageTypes.RpcResponse, payload =>
        {
            try
            {
                var reader = new BinaryBufferReader(payload);
                if (reader.RemainingBytes < CorrelationIdLength + 8 + 1 + 4)
                {
                    return;
                }

                var receivedCorrelationId = reader.ReadBytes(CorrelationIdLength);
                if (!receivedCorrelationId.AsSpan().SequenceEqual(correlationId))
                {
                    return;
                }

                var sequence = reader.ReadU64();
                var flags = reader.ReadU8();
                if ((flags & ~RpcResponseFlagStreamEnd) != 0)
                {
                    return;
                }

                var bodyLength = reader.ReadU32();
                if (reader.RemainingBytes < bodyLength)
                {
                    return;
                }

                var responseBody = reader.ReadBytes((int)bodyLength);
                var streamEnd = (flags & RpcResponseFlagStreamEnd) != 0;
                if (!reader.IsEof)
                {
                    return;
                }

                if (streamEnd && TryDecodeTerminalError(responseBody, out var rpcError))
                {
                    terminalError = ExceptionDispatchInfo.Capture(rpcError);
                    registration?.Dispose();
                    channel.Dispose();
                    return;
                }

                if (!streamEnd || responseBody.Length > 0)
                {
                    channel.PostNotification(new RpcResponseFrame(responseBody.AsMemory(), sequence));
                }

                if (streamEnd)
                {
                    registration?.Dispose();
                    channel.Dispose();
                }
            }
            catch
            {
                registration?.Dispose();
                channel.Dispose();
            }
        });

        using var writer = new BinaryBufferWriter();
        writer.WriteBytes(correlationId);
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

                if (terminalError is not null)
                {
                    terminalError.Throw();
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
            registration?.Dispose();
            channel.Dispose();
        }
    }

    public async Task<RpcWorkerRegistration> RegisterWorkerAsync(
        string pattern,
        Func<RpcRequest, IRpcResponseWriter, CancellationToken, Task> handler,
        CancellationToken ct = default)
    {
        if (!RouteValidation.IsConcreteRoute(pattern, "rpc"))
        {
            throw new RpcException($"route '{pattern}' must be a concrete rpc route", "INVALID_ROUTE");
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for worker registration");
        }

        await SubscribeWorkerAsync(pattern, ct).ConfigureAwait(false);
        _workers[pattern] = handler;
        EnsureRpcRequestHandlerInitialized();
        _workerReconnectRegistration ??= _onReconnect?.Invoke(ResubscribeWorkersAsync);

        return new RpcWorkerRegistration(pattern, unregisterToken => new ValueTask(UnsubscribeWorkerAsync(pattern, unregisterToken)));
    }

    private void EnsureRpcRequestHandlerInitialized()
    {
        if (_rpcRequestHandlerInitialized || _registerNotificationHandler == null)
        {
            return;
        }

        _rpcRequestHandlerInitialized = true;
        _registerNotificationHandler(MessageTypes.RpcRequest, payload =>
        {
            if (_dispatchAsyncHandler is not null)
            {
                if (!_dispatchAsyncHandler(token => new ValueTask(HandleIncomingRequestAsync(payload, token))))
                {
                    _ = TrySendBackpressureResponseAsync(payload);
                }

                return;
            }

            _ = HandleIncomingRequestAsync(payload, CancellationToken.None);
        });
    }

    private async Task HandleIncomingRequestAsync(byte[] payload, CancellationToken cancellationToken)
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

            var body = reader.ReadBytes((int)bodyLength);
            if (!reader.IsEof)
            {
                return;
            }

            if (!TryGetWorker(route, out var handler))
            {
                return;
            }

            var writer = new RpcResponseWriter(_send, correlationId);
            await handler(new RpcRequest(route, body), writer, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Handler exceptions are intentionally swallowed to avoid tearing down dispatch.
        }
    }

    private bool TryGetWorker(string route, out Func<RpcRequest, IRpcResponseWriter, CancellationToken, Task> handler)
    {
        if (_workers.TryGetValue(route, out handler!))
        {
            return true;
        }

        foreach (var entry in _workers)
        {
            if (RouteMatchesPattern(route, entry.Key))
            {
                handler = entry.Value;
                return true;
            }
        }

        handler = default!;
        return false;
    }

    private async Task SubscribeWorkerAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        writer.WriteU32(DefaultWorkerMaxConcurrency);

        var response = await _request(MessageTypes.RpcSubscribeWorker, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new RpcException($"REGISTER failed with status {status}", "REGISTER_FAILED", status);
        }

        if (reader.RemainingBytes >= 8)
        {
            _ = reader.ReadU64();
        }

        if (reader.RemainingBytes > 0)
        {
            _ = reader.ReadBytes(reader.RemainingBytes);
        }
    }

    private async Task UnsubscribeWorkerAsync(string pattern, CancellationToken ct)
    {
        _workers.Remove(pattern);
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        try
        {
            var response = await _request(MessageTypes.RpcUnsubscribeWorker, writer.WrittenMemory, ct).ConfigureAwait(false);
            if (response.IsEmpty)
            {
                return;
            }

            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new RpcException($"UNREGISTER failed with status {status}", "UNREGISTER_FAILED", status);
            }
        }
        finally
        {
            if (_workers.Count == 0)
            {
                _workerReconnectRegistration?.Dispose();
                _workerReconnectRegistration = null;
            }
        }
    }

    private async ValueTask ResubscribeWorkersAsync(CancellationToken cancellationToken)
    {
        foreach (var pattern in _workers.Keys.ToArray())
        {
            await SubscribeWorkerAsync(pattern, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask TrySendBackpressureResponseAsync(byte[] payload)
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

    private static bool TryDecodeInboundRequest(byte[] payload, out byte[] correlationId, out string route)
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

            _ = reader.ReadBytes((int)bodyLength);
            return reader.IsEof;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryDecodeTerminalError(ReadOnlySpan<byte> payload, out RpcException rpcError)
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
                MapRpcErrorCode(code));
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static byte[] EncodeTerminalErrorBody(uint code, string message)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU32(code);
        writer.WriteString(message);
        return writer.Build();
    }

    private static string MapRpcErrorCode(uint code)
    {
        return code switch
        {
            6001 => "TIMEOUT",
            6002 => "WORKER_NOT_FOUND",
            6003 => "BACKPRESSURE",
            6004 => "ROUTE_NOT_REGISTERED",
            6005 => "CORRELATION_NOT_FOUND",
            6006 => "INVALID_SEQUENCE",
            6007 => "DUPLICATE_CORRELATION",
            6008 => "WRONG_WORKER",
            6009 => "UNAUTHORIZED",
            6010 => "BACKEND_ERROR",
            _ => "DOMAIN_ERROR",
        };
    }

    private static bool RouteMatchesPattern(string route, string pattern)
    {
        var routeSegments = route.Split('/', StringSplitOptions.None);
        var patternSegments = pattern.Split('/', StringSplitOptions.None);

        var routeIndex = 0;
        var patternIndex = 0;

        while (patternIndex < patternSegments.Length && routeIndex < routeSegments.Length)
        {
            var segment = patternSegments[patternIndex];
            if (segment == "**")
            {
                return true;
            }

            if (segment != "*" && !string.Equals(segment, routeSegments[routeIndex], StringComparison.Ordinal))
            {
                return false;
            }

            patternIndex++;
            routeIndex++;
        }

        if (patternIndex == patternSegments.Length && routeIndex == routeSegments.Length)
        {
            return true;
        }

        return patternIndex == patternSegments.Length - 1 && patternSegments[patternIndex] == "**";
    }

    private sealed class RpcResponseWriter : IRpcResponseWriter
    {
        private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
        private readonly byte[] _correlationId;
        private ulong _sequence;

        internal RpcResponseWriter(Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send, byte[] correlationId)
        {
            _send = send;
            _correlationId = correlationId;
        }

        public async ValueTask SendAsync(ReadOnlyMemory<byte> body, bool isEnd = false, CancellationToken ct = default)
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteBytes(_correlationId);
            writer.WriteU64(_sequence++);
            writer.WriteU8(isEnd ? RpcResponseFlagStreamEnd : (byte)0);
            writer.WriteU32((uint)body.Length);
            writer.WriteBytes(body.Span);

            await _send(MessageTypes.RpcResponse, writer.WrittenMemory, ct).ConfigureAwait(false);
        }
    }

}
