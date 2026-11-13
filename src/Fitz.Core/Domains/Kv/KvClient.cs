using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Kv;

/// <summary>
/// The default <see cref="IKvClient"/>: KV transactions and key-change subscriptions.
/// </summary>
/// <remarks>
/// Obtained from <see cref="FitzClient"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class KvClient : IKvClient, IAsyncDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<Action, IDisposable>? _registerOnDisconnect;
    readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    readonly FitzConnection? _connection;
    readonly Func<bool> _supportsExclusiveScan;
    readonly TimeProvider _time;
    readonly SubscriptionTable<KvNotification> _subscriptions;
    readonly IDisposable? _connectionBinding;
    int _disposed;

    internal KvClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.OnDisconnect,
            connection.RequestWithRetryAsync,
            connection.RegisterNotificationHandler,
            (handler, rejected) => connection.TryDispatchAsyncHandler("kv", handler, rejected),
            connection.SubscriptionBufferCapacity,
            () => true,
            connection.Time)
    {
        _connection = connection;
        _connectionBinding = _subscriptions.AttachTo(connection);
    }

    /// <summary>
    /// Creates a domain client over a request delegate, for testing without a broker.
    /// </summary>
    public KvClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
        : this(async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)))
    {
        ArgumentNullException.ThrowIfNull(request);
    }

    internal KvClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retryRequest = null,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        int subscriptionBufferCapacity = SubscriptionRegistration<KvNotification>.DefaultCapacity,
        Func<bool>? supportsExclusiveScan = null,
        TimeProvider? timeProvider = null)
    {
        _request = request;
        _time = timeProvider ?? TimeProvider.System;
        _registerOnDisconnect = registerOnDisconnect;
        _retryRequest = retryRequest;
        _subscriptions = new SubscriptionTable<KvNotification>(
            this,
            "kv",
            MessageTypes.KvNotify,
            registerNotificationHandler,
            dispatchAsyncHandler,
            subscriptionBufferCapacity,
            SubscribeWireAsync,
            UnsubscribeWireAsync,
            DecodeNotification,
            _time);
        _supportsExclusiveScan = supportsExclusiveScan ?? (() => true);
    }

    /// <inheritdoc />
    public async Task<IKvTransaction> BeginAsync(
        string route,
        KvMode mode = KvMode.ReadWrite,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "kv", 3))
        {
            throw new ArgumentException($"route '{route}' must be kv://{{realm}}/{{area}}/{{resource}}", nameof(route));
        }
        if (mode is not KvMode.ReadOnly and not KvMode.ReadWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown KV transaction mode");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU8((byte)mode);

        using var session = new SessionRequestContext(_connection, _request, _registerOnDisconnect);
        var response = await session.RequestAsync(MessageTypes.KvBegin, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = KvWireHelpers.ReadSuccess(response, "BEGIN");

        if (reader.IsEof || reader.RemainingBytes < 8)
        {
            throw new KvException("BEGIN response missing transaction id", "MISSING_TX_ID");
        }

        var txId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new KvException("BEGIN response has trailing bytes", "BEGIN_INVALID_RESPONSE");
        }

        session.EnsureValid();
        return new KvTransaction(
            session.BoundRequest,
            route,
            txId,
            session.RegisterOnDisconnect,
            session.BindRetry(_retryRequest),
            _supportsExclusiveScan,
            _time);
    }

    /// <inheritdoc />
    public Task<KvSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsurePattern(pattern);
        return _subscriptions.SubscribeBufferedAsync(
            pattern,
            static (pattern, notifications, unsubscribe, completion, time) => new KvSubscription(pattern, notifications, unsubscribe, completion, time),
            preDispatch: null,
            ct);
    }

    internal async Task<KvSubscription> SubscribeAsync(
        string pattern,
        Func<KvNotification, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        EnsurePattern(pattern);
        var binding = await _subscriptions.SubscribeAsync(pattern, handler, preDispatch: null, ct).ConfigureAwait(false);
        return new KvSubscription(pattern, binding.Unsubscribe, binding.Completion, _subscriptions.Time);
    }

    /// <summary>Releases local resources and ends any registrations this client owns.</summary>
    /// <returns>A task that completes once an in-progress subscription change has finished.</returns>
    /// <remarks>Queued and later subscription changes fail with <see cref="ObjectDisposedException"/>;
    /// unsubscribes and reconnect restoration become no-ops.</remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _connectionBinding?.Dispose();
        await _subscriptions.DisposeAsync().ConfigureAwait(false);
    }

    static void EnsurePattern(string pattern)
    {
        if (!RouteValidation.IsRegistrationPattern(pattern, "kv", 3))
        {
            throw new ArgumentException($"pattern '{pattern}' must use whole-segment wildcards and match a three-segment KV route", nameof(pattern));
        }
    }

    async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.KvSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = ReadSubscriptionResponse(response, "SUBSCRIBE", expectedBytes: 8);
        return reader.ReadU64();
    }

    async Task UnsubscribeWireAsync(string pattern, ulong subscriptionId, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.KvUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        ReadSubscriptionResponse(response, "UNSUBSCRIBE", expectedBytes: 0);
    }

    static BinaryBufferReader ReadSubscriptionResponse(ReadOnlyMemory<byte> response, string operation, int expectedBytes)
    {
        var reader = KvWireHelpers.ReadSuccess(response, operation);
        if (reader.RemainingBytes != expectedBytes)
        {
            throw KvWireHelpers.InvalidResponse(operation, $"expected {expectedBytes} payload bytes, got {reader.RemainingBytes}");
        }

        return reader;
    }

    static DecodedNotification<KvNotification>? DecodeNotification(ReadOnlyMemory<byte> payload)
    {
        var reader = new BinaryBufferReader(payload);
        var subscriptionId = reader.ReadU64();
        var route = reader.ReadString();
        var mutationCount = reader.ReadU64();
        return reader.IsEof
            ? new DecodedNotification<KvNotification>(subscriptionId, route, new KvNotification(route, mutationCount))
            : null;
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
