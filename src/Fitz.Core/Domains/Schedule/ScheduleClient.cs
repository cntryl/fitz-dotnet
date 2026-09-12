using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Schedule;

/// <summary>
/// The default <see cref="IScheduleClient"/>: cron schedules and their firings.
/// </summary>
/// <remarks>
/// Obtained from <see cref="Client"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class ScheduleClient : IScheduleClient, IDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly int _subscriptionBufferCapacity;
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Client disposal may race active subscriptions; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    readonly object _gate = new();
    readonly Dictionary<string, ScheduleSubscriptionState> _subscriptionsByRoute = new(StringComparer.Ordinal);
    readonly Dictionary<ulong, string> _routesBySubscriptionId = [];
    IDisposable? _notificationRegistration;
    int _disposed;
    bool _notificationHandlerInitialized;
    long _nextHandleId;
    readonly IDisposable? _reconnectRegistration;

    internal ScheduleClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterBorrowedNotificationHandler,
            (handler, rejected) => connection.TryDispatchAsyncHandler("schedule", handler, rejected),
            connection.SubscriptionBufferCapacity)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
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
        int subscriptionBufferCapacity = SubscriptionRegistration<ScheduleNotification>.DefaultCapacity)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _subscriptionBufferCapacity = subscriptionBufferCapacity;
    }

    /// <inheritdoc />
    public async Task<string?> CreateAsync(string route, string cron, ScheduleDeliveryMode deliveryMode, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateScheduleRoute(route);
        if (deliveryMode is not ScheduleDeliveryMode.Broadcast and not ScheduleDeliveryMode.Single)
        {
            throw new ArgumentOutOfRangeException(nameof(deliveryMode), deliveryMode, "Unknown schedule delivery mode");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(cron);
        writer.WriteU8((byte)deliveryMode);
        writer.WriteU32((uint)payload.Length);
        writer.WriteBytes(payload.Span);
        var data = await AssertSuccessAsync(MessageTypes.ScheduleCreate, writer.WrittenMemory, "CREATE", ct).ConfigureAwait(false);
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
    public async Task CancelAsync(string route, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ValidateScheduleRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var data = await AssertSuccessAsync(MessageTypes.ScheduleCancel, writer.WrittenMemory, "CANCEL", ct).ConfigureAwait(false);
        if (!data.IsEmpty)
        {
            throw new ScheduleException("CANCEL response has trailing bytes", "CANCEL_INVALID_RESPONSE");
        }
    }

    /// <inheritdoc />
    public async Task<ScheduleListPage> ListAsync(ulong? offset = null, ulong? limit = null, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        using var writer = new BinaryBufferWriter();
        writer.WriteU8((byte)(offset.HasValue ? 1 : 0));
        if (offset.HasValue)
            writer.WriteU64(offset.Value);
        writer.WriteU8((byte)(limit.HasValue ? 1 : 0));
        if (limit.HasValue)
            writer.WriteU64(limit.Value);

        var response = await _request(MessageTypes.ScheduleListPage, writer.WrittenMemory, ct).ConfigureAwait(false);
        if (response.IsEmpty)
        {
            throw new ScheduleException("LIST response is empty", "LIST_INVALID_RESPONSE");
        }

        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            uint? domainCode = null;
            var message = "Schedule LIST failed";
            try
            {
                if (reader.RemainingBytes >= 4)
                {
                    domainCode = reader.ReadU32();
                    if (reader.RemainingBytes >= 4)
                    {
                        message = reader.ReadString();
                    }
                }
            }
            catch (ProtocolException)
            {
                throw new ScheduleException("LIST error response is truncated", "LIST_INVALID_RESPONSE", status, domainCode);
            }

            if (!reader.IsEof)
            {
                throw new ScheduleException("LIST error response has trailing or truncated bytes", "LIST_INVALID_RESPONSE", status, domainCode);
            }

            throw new ScheduleException($"LIST failed: {message}", "LIST_FAILED", status, domainCode);
        }
        if (reader.RemainingBytes < 9)
        {
            throw new ScheduleException("LIST response is missing its total count or entry sentinel", "LIST_INVALID_RESPONSE");
        }
        var totalCount = reader.ReadU64();
        var entries = new List<ScheduleEntry>();

        while (true)
        {
            var hasEntry = reader.ReadU8();
            if (hasEntry == 0)
            {
                break;
            }
            if (hasEntry != 1)
            {
                throw new ScheduleException($"LIST response has invalid entry sentinel {hasEntry}", "LIST_INVALID_RESPONSE");
            }

            var route = reader.ReadString();
            var cron = reader.ReadString();
            var deliveryModeValue = reader.ReadU8();
            var deliveryMode = (ScheduleDeliveryMode)deliveryModeValue;
            if (deliveryMode is not ScheduleDeliveryMode.Broadcast and not ScheduleDeliveryMode.Single)
            {
                throw new ScheduleException($"LIST response has invalid delivery mode {deliveryModeValue}", "LIST_INVALID_RESPONSE");
            }
            var payloadLength = reader.ReadU32();
            var payload = reader.ReadBytes(payloadLength);
            entries.Add(new ScheduleEntry(null, route, cron, deliveryMode, payload));
        }

        if (!reader.IsEof)
        {
            throw new ScheduleException("LIST response has trailing bytes", "LIST_INVALID_RESPONSE");
        }

        return new ScheduleListPage(entries.ToArray(), totalCount);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ScheduleEntry>> ListBySelectorAsync(string selector, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsRegistrationPattern(selector, "schedule", 4))
        {
            throw new ScheduleException($"pattern '{selector}' must use whole-segment wildcards and match a four-segment schedule route", "INVALID_ROUTE");
        }

        var entries = new List<ScheduleEntry>();
        ulong offset = 0;
        const ulong limit = 1000;
        while (true)
        {
            var page = await ListAsync(offset, limit, ct).ConfigureAwait(false);
            entries.AddRange(page.Entries.Where(entry => RouteValidation.MatchesPattern(entry.Route, selector)));
            offset += (ulong)page.Entries.Count;
            if (offset >= page.TotalCount)
            {
                return entries;
            }
            if (page.Entries.Count == 0)
                throw new ScheduleException("LIST returned an empty page before total_count", "LIST_INVALID_RESPONSE");
        }
    }

    /// <inheritdoc />
    public async Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var buffer = new AsyncSubscriptionBuffer<ScheduleNotification>(pattern, _subscriptionBufferCapacity);
        var registration = await SubscribeAsync(pattern, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, ct).ConfigureAwait(false);
        buffer.ObserveCompletion(registration.Completion);
        return new ScheduleSubscription(pattern, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            await registration.UnsubscribeAsync(token).ConfigureAwait(false);
            buffer.Complete();
        }, registration.Completion);
    }

    internal async Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        Func<ScheduleNotification, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsRegistrationPattern(pattern, "schedule", 4))
        {
            throw new ScheduleException($"pattern '{pattern}' must use whole-segment wildcards and match a four-segment schedule route", "INVALID_ROUTE");
        }
        EnsureNotificationHandlerInitialized();

        var channel = SubscriptionRegistration<ScheduleNotification>.CreateChannel(_subscriptionBufferCapacity);
        SubscriptionRegistration<ScheduleNotification>? registration = null;

        var handleId = Interlocked.Increment(ref _nextHandleId);
        var gateAcquired = false;

        try
        {
            registration = new SubscriptionRegistration<ScheduleNotification>(
                channel,
                "schedule",
                pattern,
                token => UnsubscribeAsync(pattern, handleId, token));
            await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
            gateAcquired = true;

            lock (_gate)
            {
                if (_subscriptionsByRoute.TryGetValue(pattern, out var existingSubscription))
                {
                    existingSubscription.Writers[handleId] = registration;
                    var existingHandle = CreateSubscription(pattern, handleId, registration.Completion);
                    SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                    registration = null;
                    return existingHandle;
                }
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new ScheduleSubscriptionState(subscriptionId);
            subscription.Writers[handleId] = registration;
            lock (_gate)
            {
                _subscriptionsByRoute[pattern] = subscription;
                _routesBySubscriptionId[subscriptionId] = pattern;
            }

            var handle = CreateSubscription(pattern, handleId, registration.Completion);
            SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
            registration = null;
            return handle;
        }
        finally
        {
            if (gateAcquired)
            {
                _subscriptionGate.Release();
            }

            registration?.Dispose();
        }
    }

    ScheduleSubscription CreateSubscription(
        string route,
        long handleId,
        Task completion)
    {
        return new ScheduleSubscription(
            route,
            ct => UnsubscribeAsync(route, handleId, ct),
            completion);
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership intentionally remains in the subscription table when the wire unsubscribe fails.")]
    async ValueTask UnsubscribeAsync(string route, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<ScheduleNotification>? registration = null;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var shouldUnsubscribe = false;
            lock (_gate)
            {
                if (!_subscriptionsByRoute.TryGetValue(route, out var subscription) ||
                    !subscription.Writers.TryGetValue(handleId, out registration))
                {
                    return;
                }

                shouldUnsubscribe = subscription.Writers.Count == 1;
            }

            if (shouldUnsubscribe)
            {
                await UnsubscribeWireAsync(route, ct).ConfigureAwait(false);
            }

            lock (_gate)
            {
                if (!_subscriptionsByRoute.TryGetValue(route, out var subscription) ||
                    !subscription.Writers.Remove(handleId, out registration))
                {
                    return;
                }

                if (subscription.Writers.Count == 0)
                {
                    _subscriptionsByRoute.Remove(route);
                    _routesBySubscriptionId.Remove(subscription.SubscriptionId);
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();

    }

    async Task<ulong> SubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.ScheduleSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        if (response.IsEmpty)
        {
            throw new ScheduleException("SUBSCRIBE response is empty", "SUBSCRIBE_INVALID_RESPONSE");
        }

        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            var message = reader.ReadString();
            throw new ScheduleException($"SUBSCRIBE failed: {message}", "SUBSCRIBE_FAILED", status);
        }

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

    async Task UnsubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var data = await AssertSuccessAsync(MessageTypes.ScheduleUnsubscribe, writer.WrittenMemory, "UNSUBSCRIBE", ct).ConfigureAwait(false);
        if (!data.IsEmpty)
        {
            throw new ScheduleException("UNSUBSCRIBE response has trailing bytes", "UNSUBSCRIBE_INVALID_RESPONSE");
        }
    }

    void EnsureNotificationHandlerInitialized()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_notificationHandlerInitialized)
            {
                return;
            }

            if (_registerNotificationHandler == null)
            {
                throw new InvalidOperationException("Notification handlers not configured for subscription support");
            }

            _notificationRegistration = _registerNotificationHandler(MessageTypes.ScheduleNotify, HandleNotification);
            _notificationHandlerInitialized = true;
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed broker notifications are dropped without disrupting the receive loop.")]
    void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var exactRoute = reader.ReadString();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes(bodyLength);
            if (!reader.IsEof)
            {
                return;
            }

            SubscriptionRegistration<ScheduleNotification>[] registrations;
            lock (_gate)
            {
                if (!_routesBySubscriptionId.TryGetValue(subscriptionId, out var route) ||
                    !_subscriptionsByRoute.TryGetValue(route, out var subscription))
                {
                    return;
                }

                registrations = [.. subscription.Writers.Values];
            }

            var notification = new ScheduleNotification(exactRoute, body);
            foreach (var registration in registrations)
            {
                if (!registration.Channel.Writer.TryWrite(notification))
                    _ = registration.FailOverflowAsync();
            }
        }
        catch
        {
        }
    }

    async ValueTask HandleReconnect(CancellationToken ct) => await RestoreSubscriptionsAsync(ct).ConfigureAwait(false);

    static void ValidateScheduleRoute(string route)
    {
        if (RouteValidation.TryValidateFixedRoute(route, "schedule", 4, out var failure))
        {
            return;
        }

        throw CreateRouteException(route, failure);
    }

    static ScheduleException CreateRouteException(string route, RouteValidationFailure failure)
    {
        var message = failure switch
        {
            RouteValidationFailure.InvalidScheme => $"schedule route '{route}' must start with schedule://",
            RouteValidationFailure.EmptySegment => $"schedule route '{route}' segments must be non-empty",
            RouteValidationFailure.ContainsWildcard => $"schedule route '{route}' must not contain wildcards",
            _ => $"schedule route '{route}' must be schedule://{{realm}}/{{area}}/{{resource}}/{{operation}}",
        };

        return new ScheduleException(message, "INVALID_ROUTE");
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect restoration must best-effort roll back every already-restored subscription before preserving the original failure.")]
    async ValueTask RestoreSubscriptionsAsync(CancellationToken ct)
    {
        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            List<(string Route, ScheduleSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByRoute.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Route, ScheduleSubscriptionState Subscription)>(_subscriptionsByRoute.Count);
                foreach (var entry in _subscriptionsByRoute)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, ScheduleSubscriptionState>(StringComparer.Ordinal);
            var restoredRoutesById = new Dictionary<ulong, string>();

            try
            {
                foreach (var entry in snapshot)
                {
                    var subscriptionId = await SubscribeWireAsync(entry.Route, ct).ConfigureAwait(false);
                    restoredSubscriptions[entry.Route] = entry.Subscription.Clone(subscriptionId);
                    restoredRoutesById[subscriptionId] = entry.Route;
                }
            }
            catch
            {
                foreach (var route in restoredSubscriptions.Keys)
                {
                    try
                    {
                        await UnsubscribeWireAsync(route, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Best effort; preserve the original restore failure.
                    }
                }

                throw;
            }

            lock (_gate)
            {
                _subscriptionsByRoute.Clear();
                _routesBySubscriptionId.Clear();

                foreach (var entry in restoredSubscriptions)
                {
                    _subscriptionsByRoute[entry.Key] = entry.Value;
                }

                foreach (var entry in restoredRoutesById)
                {
                    _routesBySubscriptionId[entry.Key] = entry.Value;
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    /// <summary>Releases local resources and ends any registrations this client owns.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _notificationRegistration?.Dispose();
        _reconnectRegistration?.Dispose();
        lock (_gate)
        {
            foreach (var subscription in _subscriptionsByRoute.Values)
            {
                foreach (var registration in subscription.Writers.Values)
                {
                    registration.Dispose();
                }
            }

            _subscriptionsByRoute.Clear();
            _routesBySubscriptionId.Clear();
        }

    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    sealed class ScheduleSubscriptionState
    {
        public ScheduleSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; init; }

        public Dictionary<long, SubscriptionRegistration<ScheduleNotification>> Writers { get; } = [];

        public ScheduleSubscriptionState Clone() => Clone(SubscriptionId);

        public ScheduleSubscriptionState Clone(ulong subscriptionId)
        {
            var clone = new ScheduleSubscriptionState(subscriptionId);
            foreach (var entry in Writers)
            {
                clone.Writers.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }

    async ValueTask<ReadOnlyMemory<byte>> AssertSuccessAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        if (response.IsEmpty)
        {
            throw new ScheduleException($"{operation} response is empty", $"{operation}_INVALID_RESPONSE");
        }

        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            if (status == 1)
            {
                var message = reader.ReadString();
                if (!reader.IsEof)
                {
                    throw new ScheduleException($"{operation} error response has trailing bytes", $"{operation}_INVALID_RESPONSE", status);
                }
                throw new ScheduleException($"{operation} failed: {message}", $"{operation}_FAILED", status);
            }
            throw new ScheduleException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        return reader.IsEof ? ReadOnlyMemory<byte>.Empty : response.Slice(1);
    }
}
