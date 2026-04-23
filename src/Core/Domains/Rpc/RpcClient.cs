using System.Runtime.CompilerServices;
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
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> _send;
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;
    private readonly Func<Func<CancellationToken, ValueTask>, IDisposable>? _onReconnect;
    private readonly Func<CancellationToken>? _getConnectionClosedToken;
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
            connectionTimeout: connection.Timeout)
    {
    }

    public RpcClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, byte[], CancellationToken, Task>? send = null,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, IDisposable>? onReconnect = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        TimeSpan? connectionTimeout = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            send is null
                ? async (messageType, payload, ct) => { _ = await request(messageType, payload.ToArray(), ct).ConfigureAwait(false); }
    : async (messageType, payload, ct) => await send(messageType, payload.ToArray(), ct).ConfigureAwait(false),
            registerNotificationHandler,
            onReconnect,
            getConnectionClosedToken,
            connectionTimeout)
    {
    }

    internal RpcClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, IDisposable>? onReconnect = null,
        Func<CancellationToken>? getConnectionClosedToken = null,
        TimeSpan? connectionTimeout = null)
    {
        _request = request;
        _send = send;
        _registerNotificationHandler = registerNotificationHandler;
        _onReconnect = onReconnect;
        _getConnectionClosedToken = getConnectionClosedToken;
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
        IDisposable? registration = null;
        registration = _registerNotificationHandler(MessageTypes.RpcResponse, payload =>
        {
            try
            {
                var reader = new BinaryBufferReader(payload);
                var corrLen = reader.ReadU32();
                if (corrLen != 16)
                {
                    return;
                }

                var receivedCorrelationId = reader.ReadBytes((int)corrLen);
                if (!receivedCorrelationId.AsSpan().SequenceEqual(correlationId))
                {
                    return;
                }

                var sequence = reader.ReadU64();
                var bodyLength = reader.ReadU32();
                var responseBody = reader.ReadBytes((int)bodyLength);
                var streamEnd = !reader.IsEof && reader.ReadU8() == 1;

                channel.PostNotification(new RpcResponseFrame(responseBody.AsMemory(), sequence));
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
        writer.WriteU32((uint)correlationId.Length);
        writer.WriteBytes(correlationId);
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);

        try
        {
            var response = await _request(MessageTypes.RpcRequest, writer.WrittenMemory, ct).ConfigureAwait(false);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new RpcException($"CALL failed with status {status}", "CALL_FAILED", status);
            }

            if (reader.RemainingBytes > 0)
            {
                _ = reader.ReadBytes(reader.RemainingBytes);
            }

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
            registration?.Dispose();
            channel.Dispose();
        }
    }

    public async Task<IDisposable> RegisterWorkerAsync(
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

        return new RpcWorkerRegistration(this, pattern);
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
            _ = HandleIncomingRequestAsync(payload);
        });
    }

    private async Task HandleIncomingRequestAsync(byte[] payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var corrLen = reader.ReadU32();
            if (corrLen != 16)
            {
                return;
            }

            var correlationId = reader.ReadBytes((int)corrLen);
            var route = reader.ReadString();
            _ = reader.ReadString(); // reply route, currently unused by the broker.
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes((int)bodyLength);

            if (!TryGetWorker(route, out var handler))
            {
                return;
            }

            var writer = new RpcResponseWriter(_send, correlationId);
            await handler(new RpcRequest(route, body), writer, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"RPC handler error: {ex}");
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

    private async Task UnsubscribeWorkerAsync(string pattern)
    {
        _workers.Remove(pattern);
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        try
        {
            var response = await _request(MessageTypes.RpcUnsubscribeWorker, writer.WrittenMemory, CancellationToken.None).ConfigureAwait(false);
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

            if (!reader.IsEof)
            {
                throw new RpcException("UNREGISTER response has trailing bytes", "UNREGISTER_INVALID_RESPONSE");
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
            writer.WriteU32((uint)_correlationId.Length);
            writer.WriteBytes(_correlationId);
            writer.WriteU64(_sequence++);
            writer.WriteU32((uint)body.Length);
            writer.WriteBytes(body.Span);
            writer.WriteU8(isEnd ? (byte)1 : (byte)0);

            await _send(MessageTypes.RpcResponse, writer.WrittenMemory, ct).ConfigureAwait(false);
        }
    }

    private sealed class RpcWorkerRegistration : IDisposable
    {
        private readonly RpcClient _owner;
        private readonly string _pattern;
        private int _disposed;

        internal RpcWorkerRegistration(RpcClient owner, string pattern)
        {
            _owner = owner;
            _pattern = pattern;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _ = _owner.UnsubscribeWorkerAsync(_pattern);
        }
    }
}
