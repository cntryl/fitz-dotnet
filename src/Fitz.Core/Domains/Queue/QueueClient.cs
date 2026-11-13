using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Queue;

/// <summary>
/// The default <see cref="IQueueClient"/>: queue enqueue, reserve, and availability subscriptions.
/// </summary>
/// <remarks>
/// Obtained from <see cref="FitzClient"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class QueueClient : IQueueClient, IAsyncDisposable
{
    // The broker returns at most this many items per RESERVE (queue spec, "RESERVE Long Polling").
    const int MaxReserveBatch = 1024;

    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly FitzConnection? _connection;
    readonly Func<Action, IDisposable>? _registerOnDisconnect;
    readonly SubscriptionTable<QueueAvailabilityEvent> _subscriptions;
    readonly IDisposable? _connectionBinding;
    readonly Func<ushort, ReadOnlyMemory<byte>, byte, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _requestWithFlags;
    readonly Func<uint, bool>? _supportsCapability;
    int _disposed;

    internal QueueClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.OnDisconnect,
            (handler, rejected) => connection.TryDispatchAsyncHandler("queue", handler, rejected),
            connection.SubscriptionBufferCapacity,
            connection.Time)
    {
        _requestWithFlags = connection.RequestWithFlagsAsync;
        _supportsCapability = id => connection.Capabilities.SupportsCapability(id);
        _connection = connection;
        _connectionBinding = _subscriptions.AttachTo(connection);
    }

    /// <summary>
    /// Creates a domain client over a request delegate, for testing without a broker.
    /// </summary>
    public QueueClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
        ArgumentNullException.ThrowIfNull(request);
    }

    internal QueueClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        int subscriptionBufferCapacity = SubscriptionRegistration<QueueAvailabilityEvent>.DefaultCapacity,
        TimeProvider? timeProvider = null)
    {
        _request = request;
        _registerOnDisconnect = registerOnDisconnect;
        _subscriptions = new SubscriptionTable<QueueAvailabilityEvent>(
            this,
            "queue",
            MessageTypes.QueueNotify,
            registerNotificationHandler,
            dispatchAsyncHandler,
            subscriptionBufferCapacity,
            SubscribeWireAsync,
            UnsubscribeWireAsync,
            DecodeNotification,
            timeProvider);
    }

    /// <inheritdoc />
    public Task<ulong> EnqueueAsync(
        string route,
        ReadOnlyMemory<byte> body,
        TimeSpan? delay = null,
        CancellationToken ct = default)
        => EnqueueCoreAsync(route, body, delay, null, ct);

    /// <inheritdoc />
    public Task<ulong> EnqueueAsync(string route, ReadOnlyMemory<byte> body, QueueEnqueueOptions options, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        uint? ttlHours = options.TimeToLive is { } timeToLive ? ToWholeHours(timeToLive, nameof(options)) : null;
        return EnqueueCoreAsync(route, body, options.Delay, ttlHours, ct);
    }

    static uint ToWholeHours(TimeSpan timeToLive, string paramName)
    {
        if (timeToLive <= TimeSpan.Zero ||
            timeToLive.Ticks % TimeSpan.TicksPerHour != 0 ||
            timeToLive.Ticks / TimeSpan.TicksPerHour > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                paramName,
                timeToLive,
                $"{nameof(QueueEnqueueOptions.TimeToLive)} must be a positive whole number of hours.");
        }

        return (uint)(timeToLive.Ticks / TimeSpan.TicksPerHour);
    }

    async Task<ulong> EnqueueCoreAsync(string route, ReadOnlyMemory<byte> body, TimeSpan? delay, uint? configuredTtlHours, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "queue", 3))
        {
            throw new ArgumentException($"route '{route}' must be queue://{{realm}}/{{area}}/{{resource}}", nameof(route));
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteLengthPrefixed(body.Span);

        var delaySeconds = delay is { } configuredDelay ? WireDuration.ToSeconds(configuredDelay, nameof(delay)) : 0;
        writer.WriteU8((byte)(delaySeconds > 0 ? 1 : 0));
        if (delaySeconds > 0)
        {
            writer.WriteU64(delaySeconds);
        }

        ReadOnlyMemory<byte> response;
        if (configuredTtlHours is { } ttlHours)
        {
            if (_supportsCapability?.Invoke(1) != true || _requestWithFlags is null)
                throw new NotSupportedException("Queue TTL capability was not negotiated.");
            var ttl = new byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(ttl, ttlHours);
            var extended = ExtensionCodec.Encode(writer.WrittenMemory.Span, 1, ttl);
            response = await _requestWithFlags(MessageTypes.QueueEnqueue, extended, ExtensionCodec.PresentFlag, ct).ConfigureAwait(false);
        }
        else
        {
            response = await _request(MessageTypes.QueueEnqueue, writer.WrittenMemory, ct).ConfigureAwait(false);
        }
        var reader = QueueWireHelpers.ReadSuccess(response, "ENQUEUE");

        var result = reader.IsEof ? 0UL : reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new QueueException("ENQUEUE response has trailing bytes", "ENQUEUE_INVALID_RESPONSE");
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<IQueueReservedItem[]> ReserveAsync(
        string route,
        TimeSpan lease,
        int batchSize = 1,
        TimeSpan? wait = null,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(route);
        var leaseSeconds = WireDuration.ToSeconds(lease, nameof(lease));
        var waitSeconds = wait is { } configuredWait ? (ulong?)WireDuration.ToSeconds(configuredWait, nameof(wait)) : null;
        if (!RouteValidation.IsRegistrationPattern(route, "queue", 3))
        {
            throw new ArgumentException($"route '{route}' must be a concrete queue route or a whole-segment wildcard pattern", nameof(route));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1, nameof(batchSize));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(batchSize, MaxReserveBatch, nameof(batchSize));
        ArgumentOutOfRangeException.ThrowIfZero(leaseSeconds, nameof(lease));

        return await ReserveOnceAsync(route, leaseSeconds, batchSize, waitSeconds, ct).ConfigureAwait(false);
    }

    async Task<IQueueReservedItem[]> ReserveOnceAsync(
        string route,
        ulong leaseSeconds,
        int batchSize,
        ulong? waitSeconds,
        CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(leaseSeconds);
        writer.WriteU8((byte)(batchSize > 0 ? 1 : 0));
        if (batchSize > 0)
        {
            writer.WriteU32((uint)batchSize);
        }

        if (waitSeconds > 0)
        {
            writer.WriteU8(1);
            writer.WriteU64(waitSeconds.Value);
        }
        else
        {
            writer.WriteU8(0);
        }

        using var session = new SessionRequestContext(_connection, _request, _registerOnDisconnect);
        // The broker parks a waiting RESERVE for up to wait_seconds before answering.
        var parkedFor = TimeSpan.FromSeconds(waitSeconds ?? 0UL);
        var response = await session.RequestAsync(MessageTypes.QueueReserve, writer.WrittenMemory, ct, parkedFor).ConfigureAwait(false);
        var reader = QueueWireHelpers.ReadSuccess(response, "RESERVE");

        var wildcard = route.Split('/').Any(static segment => segment.Contains('*', StringComparison.Ordinal));
        var count = reader.IsEof ? 0U : reader.ReadU32();
        var minimumItemSize = wildcard ? 24 : 20;
        if (count > (uint)(reader.RemainingBytes / minimumItemSize))
        {
            throw new QueueException("RESERVE response item count exceeds the remaining payload", "RESERVE_INVALID_RESPONSE");
        }
        var items = new IQueueReservedItem[count];
        for (var i = 0; i < count; i++)
        {
            var itemRoute = wildcard ? reader.ReadString() : route;
            if (!RouteValidation.IsFixedRoute(itemRoute, "queue", 3))
            {
                throw new QueueException($"RESERVE response contains invalid concrete queue route '{itemRoute}'", "RESERVE_INVALID_RESPONSE");
            }
            var itemId = reader.ReadU64();
            var itemToken = reader.ReadU64();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes(bodyLength);

            items[i] = new QueueReservedItem(
                itemRoute,
                body,
                itemId,
                itemToken,
                session.BoundRequest,
                session.RegisterOnDisconnect,
                _supportsCapability);
        }

        if (!reader.IsEof)
        {
            throw new QueueException("RESERVE response has trailing bytes", "RESERVE_INVALID_RESPONSE");
        }

        session.EnsureValid();
        return items;
    }

    /// <inheritdoc />
    public Task<QueueSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsurePattern(pattern);
        return _subscriptions.SubscribeBufferedAsync(
            pattern,
            static (pattern, notifications, unsubscribe, completion, time) => new QueueSubscription(pattern, notifications, unsubscribe, completion, time),
            preDispatch: null,
            ct);
    }

    internal async Task<QueueSubscription> SubscribeAsync(
        string pattern,
        Func<QueueAvailabilityEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        EnsurePattern(pattern);
        var binding = await _subscriptions.SubscribeAsync(pattern, handler, preDispatch: null, ct).ConfigureAwait(false);
        return new QueueSubscription(pattern, binding.Unsubscribe, binding.Completion, _subscriptions.Time);
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
        if (!RouteValidation.IsRegistrationPattern(pattern, "queue", 3))
        {
            throw new ArgumentException($"pattern '{pattern}' must use whole-segment wildcards and match a three-segment queue route", nameof(pattern));
        }
    }

    async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.QueueSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = QueueWireHelpers.ReadSuccess(response, "SUBSCRIBE");

        if (reader.RemainingBytes != 9 || reader.ReadU8() != 1)
        {
            throw new QueueException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        var subscriptionId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new QueueException("SUBSCRIBE response has trailing bytes", "SUBSCRIBE_INVALID_RESPONSE");
        }

        return subscriptionId;
    }

    async Task UnsubscribeWireAsync(string pattern, ulong subscriptionId, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.QueueUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = QueueWireHelpers.ReadSuccess(response, "UNSUBSCRIBE");

        if (!reader.IsEof)
        {
            throw new QueueException("UNSUBSCRIBE response has trailing bytes", "UNSUBSCRIBE_INVALID_RESPONSE");
        }
    }

    static DecodedNotification<QueueAvailabilityEvent>? DecodeNotification(ReadOnlyMemory<byte> payload)
    {
        var reader = new BinaryBufferReader(payload);
        var subscriptionId = reader.ReadU64();
        var route = reader.ReadString();
        // Queue NOTIFY carries three fixed-width u64 counters, without a byte-length prefix.
        var ready = reader.ReadU64();
        var delayed = reader.ReadU64();
        var inFlight = reader.ReadU64();
        return reader.IsEof
            ? new DecodedNotification<QueueAvailabilityEvent>(subscriptionId, route, new QueueAvailabilityEvent(route, ready, delayed, inFlight))
            : null;
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
