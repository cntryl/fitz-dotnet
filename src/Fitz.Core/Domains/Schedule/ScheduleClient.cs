using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Schedule;

/// <summary>
/// The default <see cref="IScheduleClient"/>: cron schedules and their firings.
/// </summary>
/// <remarks>
/// Obtained from <see cref="FitzClient"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class ScheduleClient : IScheduleClient, IAsyncDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    int _disposed;
    readonly SubscriptionTable<ScheduleNotification> _subscriptions;
    readonly IDisposable? _connectionBinding;

    internal ScheduleClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            (handler, rejected) => connection.TryDispatchAsyncHandler("schedule", handler, rejected),
            connection.SubscriptionBufferCapacity,
            connection.Time)
    {
        _connectionBinding = _subscriptions.AttachTo(connection);
    }

    /// <summary>
    /// Creates a domain client over a request delegate, for testing without a broker.
    /// </summary>
    public ScheduleClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(
            async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)),
            NotificationRegistrationAdapter.Adapt(registerNotificationHandler))
    {
        ArgumentNullException.ThrowIfNull(request);
    }

    internal ScheduleClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        AsyncHandlerDispatch? dispatchAsyncHandler = null,
        int subscriptionBufferCapacity = SubscriptionRegistration<ScheduleNotification>.DefaultCapacity,
        TimeProvider? timeProvider = null)
    {
        _request = request;
        _subscriptions = new SubscriptionTable<ScheduleNotification>(
            this,
            "schedule",
            MessageTypes.ScheduleNotify,
            registerNotificationHandler,
            dispatchAsyncHandler,
            subscriptionBufferCapacity,
            SubscribeWireAsync,
            UnsubscribeWireAsync,
            DecodeNotification,
            timeProvider);
    }

    /// <inheritdoc />
    public async Task<string?> CreateAsync(string route, string cron, ScheduleDeliveryMode deliveryMode, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateScheduleRoute(route, nameof(route));
        if (deliveryMode is not ScheduleDeliveryMode.Broadcast and not ScheduleDeliveryMode.Once)
        {
            throw new ArgumentOutOfRangeException(nameof(deliveryMode), deliveryMode, "Unknown schedule delivery mode");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(cron);
        writer.WriteU8((byte)deliveryMode);
        writer.WriteLengthPrefixed(payload.Span);
        var data = await RequestSuccessAsync(MessageTypes.ScheduleCreate, writer.WrittenMemory, "CREATE", ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(data);
        if (!reader.IsEof)
        {
            var hasCreatedRoute = reader.ReadU8();
            if (hasCreatedRoute > 1)
            {
                throw new ScheduleException(
                    $"CREATE response has invalid created route flag {hasCreatedRoute}",
                    "CREATE_INVALID_RESPONSE");
            }

            if (hasCreatedRoute == 1)
            {
                var createdRoute = reader.ReadString();
                if (!reader.IsEof)
                {
                    throw new ScheduleException("CREATE response has trailing bytes", "CREATE_INVALID_RESPONSE");
                }

                return createdRoute;
            }
        }

        if (!reader.IsEof)
        {
            throw new ScheduleException("CREATE response has trailing bytes", "CREATE_INVALID_RESPONSE");
        }

        return route;
    }

    /// <inheritdoc />
    public async Task CreateBatchAsync(IReadOnlyList<ScheduleEntry> entries, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(entries);
        using var writer = new BinaryBufferWriter();
        writer.WriteU32(checked((uint)entries.Count));
        foreach (var entry in entries)
        {
            ValidateScheduleRoute(entry.Route, nameof(entries));
            if (entry.DeliveryMode is not ScheduleDeliveryMode.Broadcast and not ScheduleDeliveryMode.Once)
                throw new ArgumentOutOfRangeException(nameof(entries), "Unknown schedule delivery mode");
            writer.WriteString(entry.Route);
            writer.WriteString(entry.Cron);
            writer.WriteU8((byte)entry.DeliveryMode);
            writer.WriteU32(checked((uint)entry.Payload.Length));
            writer.WriteBytes(entry.Payload.Span);
        }

        var data = await RequestSuccessAsync(MessageTypes.ScheduleCreateBatch, writer.WrittenMemory, "CREATE_BATCH", ct).ConfigureAwait(false);
        if (!data.IsEmpty)
            throw new ScheduleException("CREATE_BATCH response has trailing bytes", "CREATE_BATCH_INVALID_RESPONSE");
    }

    /// <inheritdoc />
    public async Task<ScheduleListPage> ListAsync(string? continuation = null, ulong? limit = null, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(continuation is null ? (byte)0 : (byte)1);
        if (continuation is not null)
            writer.WriteString(continuation);
        writer.WriteU8(limit.HasValue ? (byte)1 : (byte)0);
        if (limit.HasValue)
            writer.WriteU64(limit.Value);

        var data = await RequestSuccessAsync(MessageTypes.ScheduleList, writer.WrittenMemory, "LIST", ct).ConfigureAwait(false);
        try
        {
            var reader = new BinaryBufferReader(data);
            if (reader.ReadU8() != 1)
                throw new ScheduleException("LIST response has an unknown version", "LIST_INVALID_RESPONSE");
            var more = reader.ReadU8();
            if (more > 1)
                throw new ScheduleException("LIST response has an invalid has_more flag", "LIST_INVALID_RESPONSE");
            var hasContinuation = reader.ReadU8();
            if (hasContinuation > 1)
                throw new ScheduleException("LIST response has an invalid continuation flag", "LIST_INVALID_RESPONSE");
            var next = hasContinuation == 1 ? reader.ReadString() : null;
            var entries = new List<ScheduleEntry>();
            while (true)
            {
                var hasEntry = reader.ReadU8();
                if (hasEntry == 0)
                    break;
                if (hasEntry != 1)
                    throw new ScheduleException("LIST response has an invalid entry flag", "LIST_INVALID_RESPONSE");
                var route = reader.ReadString();
                var cron = reader.ReadString();
                var mode = reader.ReadU8();
                if (mode > 1)
                    throw new ScheduleException("LIST response has an invalid delivery mode", "LIST_INVALID_RESPONSE");
                var payload = reader.ReadLengthPrefixed();
                entries.Add(new ScheduleEntry(null, route, cron, (ScheduleDeliveryMode)mode, payload));
            }
            if (!reader.IsEof)
                throw new ScheduleException("LIST response has trailing bytes", "LIST_INVALID_RESPONSE");
            if ((more == 1) != (next is not null))
                throw new ScheduleException("LIST response has inconsistent cursor state", "LIST_INVALID_RESPONSE");
            return new ScheduleListPage(entries, more == 1, next);
        }
        catch (ProtocolException)
        {
            throw new ScheduleException("LIST response is truncated", "LIST_INVALID_RESPONSE");
        }
    }

    /// <inheritdoc />
    public async Task CancelAsync(string route, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateScheduleRoute(route, nameof(route));

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var data = await RequestSuccessAsync(MessageTypes.ScheduleCancel, writer.WrittenMemory, "CANCEL", ct).ConfigureAwait(false);
        if (!data.IsEmpty)
        {
            throw new ScheduleException("CANCEL response has trailing bytes", "CANCEL_INVALID_RESPONSE");
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ScheduleEntry>> ListBySelectorAsync(string selector, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsRegistrationPattern(selector, "schedule", 4))
        {
            throw new ArgumentException($"pattern '{selector}' must use whole-segment wildcards and match a four-segment schedule route", nameof(selector));
        }

        // The broker has no selector filter on LIST, so this pages the whole keyspace and
        // filters locally. Every cursor must be new: a repeated one would page forever.
        const ulong pageSize = 1000;
        var entries = new List<ScheduleEntry>();
        var seenCursors = new HashSet<string>(StringComparer.Ordinal);
        string? continuation = null;
        while (true)
        {
            var page = await ListAsync(continuation, pageSize, ct).ConfigureAwait(false);
            entries.AddRange(page.Entries.Where(entry => RouteValidation.MatchesPattern(entry.Route, selector)));
            if (!page.HasMore)
            {
                return entries;
            }
            if (page.Entries.Count == 0)
                throw new ScheduleException("LIST returned an empty page with another cursor", "LIST_INVALID_RESPONSE");
            if (page.Continuation is not { } next || !seenCursors.Add(next))
                throw new ScheduleException("LIST returned a missing or repeated cursor", "LIST_INVALID_RESPONSE");
            continuation = next;
        }
    }

    /// <inheritdoc />
    public Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        EnsurePattern(pattern);
        return _subscriptions.SubscribeBufferedAsync(
            pattern,
            static (pattern, notifications, unsubscribe, completion, time) => new ScheduleSubscription(pattern, notifications, unsubscribe, completion, time),
            preDispatch: null,
            ct);
    }

    internal async Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        Func<ScheduleNotification, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        EnsurePattern(pattern);
        var binding = await _subscriptions.SubscribeAsync(pattern, handler, preDispatch: null, ct).ConfigureAwait(false);
        return new ScheduleSubscription(pattern, binding.Unsubscribe, binding.Completion, _subscriptions.Time);
    }

    static void EnsurePattern(string pattern)
    {
        if (!RouteValidation.IsRegistrationPattern(pattern, "schedule", 4))
        {
            throw new ArgumentException($"pattern '{pattern}' must use whole-segment wildcards and match a four-segment schedule route", nameof(pattern));
        }
    }

    async Task<ulong> SubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.ScheduleSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = ScheduleWireHelpers.ReadSuccess(response, "SUBSCRIBE");
        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new ScheduleException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        var subscriptionId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new ScheduleException("SUBSCRIBE response has trailing bytes", "SUBSCRIBE_INVALID_RESPONSE");
        }

        return subscriptionId;
    }

    async Task UnsubscribeWireAsync(string route, ulong subscriptionId, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var data = await RequestSuccessAsync(MessageTypes.ScheduleUnsubscribe, writer.WrittenMemory, "UNSUBSCRIBE", ct).ConfigureAwait(false);
        if (!data.IsEmpty)
        {
            throw new ScheduleException("UNSUBSCRIBE response has trailing bytes", "UNSUBSCRIBE_INVALID_RESPONSE");
        }
    }

    static DecodedNotification<ScheduleNotification>? DecodeNotification(ReadOnlyMemory<byte> payload)
    {
        var reader = new BinaryBufferReader(payload);
        var subscriptionId = reader.ReadU64();
        var route = reader.ReadString();
        var bodyLength = reader.ReadU32();
        var body = reader.ReadBytes(bodyLength);
        return reader.IsEof
            ? new DecodedNotification<ScheduleNotification>(subscriptionId, route, new ScheduleNotification(route, body))
            : null;
    }

    static void ValidateScheduleRoute(string route, string paramName)
    {
        if (RouteValidation.TryValidateFixedRoute(route, "schedule", 4, out var failure))
        {
            return;
        }

        throw CreateRouteException(route, failure, paramName);
    }

    static ArgumentException CreateRouteException(string route, RouteValidationFailure failure, string paramName)
    {
        var message = failure switch
        {
            RouteValidationFailure.InvalidScheme => $"schedule route '{route}' must start with schedule://",
            RouteValidationFailure.EmptySegment => $"schedule route '{route}' segments must be non-empty",
            RouteValidationFailure.ContainsWildcard => $"schedule route '{route}' must not contain wildcards",
            _ => $"schedule route '{route}' must be schedule://{{realm}}/{{area}}/{{resource}}/{{operation}}",
        };

        return new ArgumentException(message, paramName);
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

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    // Returns the success payload after the status byte.
    async ValueTask<ReadOnlyMemory<byte>> RequestSuccessAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        _ = ScheduleWireHelpers.ReadSuccess(response, operation);
        return response[1..];
    }
}
