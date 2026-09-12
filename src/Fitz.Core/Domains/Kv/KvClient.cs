using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Kv;

/// <summary>
/// The default <see cref="IKvClient"/>: KV transactions and key-change subscriptions.
/// </summary>
/// <remarks>
/// Obtained from <see cref="Client"/> rather than constructed directly. The public
/// constructors exist for testing against a transport delegate.
/// </remarks>
sealed class KvClient : IKvClient, IDisposable
{
    readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    readonly Func<Action, IDisposable>? _registerOnDisconnect;
    readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly int _subscriptionBufferCapacity;
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Client disposal may race active subscriptions; SemaphoreSlim has no resource to release unless AvailableWaitHandle is used.")]
    readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    readonly object _gate = new();
    readonly Dictionary<string, KvSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    readonly Dictionary<ulong, string> _patternsBySubscriptionId = [];
    IDisposable? _notificationRegistration;
    int _disposed;
    readonly IDisposable? _reconnectRegistration;
    bool _notificationHandlerInitialized;
    long _nextHandleId;

    internal KvClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.OnDisconnect,
            (operation, messageType, payload, ct) =>
                connection.ExecuteWithRetryAsync(
                    operation,
                    innerToken => connection.RequestAsync(messageType, payload, innerToken),
                    ct),
            connection.RegisterBorrowedNotificationHandler,
            (handler, rejected) => connection.TryDispatchAsyncHandler("kv", handler, rejected),
            connection.SubscriptionBufferCapacity)
    {
        _reconnectRegistration = connection.OnReconnect(RestoreSubscriptionsAsync);
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
        int subscriptionBufferCapacity = SubscriptionRegistration<KvNotification>.DefaultCapacity)
    {
        _request = request;
        _registerOnDisconnect = registerOnDisconnect;
        _retryRequest = retryRequest;
        _registerNotificationHandler = registerNotificationHandler;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        _subscriptionBufferCapacity = subscriptionBufferCapacity;
    }

    /// <inheritdoc />
    public async Task<IKvTransaction> BeginAsync(
        string route,
        KvDurability durability,
        KvMode mode = KvMode.ReadWrite,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!RouteValidation.IsFixedRoute(route, "kv", 3))
        {
            throw new KvException($"route '{route}' must be kv://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }
        if (mode is not KvMode.ReadOnly and not KvMode.ReadWrite)
        {
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown KV transaction mode");
        }
        if (durability is not KvDurability.Async and not KvDurability.Sync)
        {
            throw new ArgumentOutOfRangeException(nameof(durability), durability, "Unknown KV durability mode");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU8((byte)mode);
        writer.WriteU8((byte)durability);

        var response = await _request(MessageTypes.KvBegin, writer.WrittenMemory, ct).ConfigureAwait(false);
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

        return new KvTransaction(_request, route, txId, _registerOnDisconnect, _retryRequest);
    }

    /// <inheritdoc />
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The returned enumerable handle owns and disposes the callback registration.")]
    public async Task<KvSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();
        var buffer = new AsyncSubscriptionBuffer<KvNotification>(pattern, _subscriptionBufferCapacity);
        var registration = await SubscribeAsync(pattern, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, ct).ConfigureAwait(false);
        buffer.ObserveCompletion(registration.Completion);
        return new KvSubscription(pattern, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            await registration.UnsubscribeAsync(token).ConfigureAwait(false);
            buffer.Complete();
        }, registration.Completion);
    }

    internal async Task<KvSubscription> SubscribeAsync(
        string pattern,
        Func<KvNotification, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsRegistrationPattern(pattern, "kv", 3))
        {
            throw new KvException($"pattern '{pattern}' must use whole-segment wildcards and match a three-segment KV route", "INVALID_ROUTE");
        }
        EnsureNotificationHandlerInitialized();

        var channel = SubscriptionRegistration<KvNotification>.CreateChannel(_subscriptionBufferCapacity);
        var handleId = Interlocked.Increment(ref _nextHandleId);
        SubscriptionRegistration<KvNotification>? registration = new(
            channel,
            "kv",
            pattern,
            token => UnsubscribeAsync(pattern, handleId, token));
        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            KvSubscriptionState? state;
            lock (_gate)
            {
                _subscriptionsByPattern.TryGetValue(pattern, out state);
            }
            if (state is null)
            {
                var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
                state = new KvSubscriptionState(subscriptionId);
                lock (_gate)
                {
                    _subscriptionsByPattern[pattern] = state;
                    _patternsBySubscriptionId[subscriptionId] = pattern;
                }
            }
            lock (_gate)
            {
                state.Writers[handleId] = registration;
            }
            SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
            var completion = registration.Completion;
            registration = null;
            return new KvSubscription(
                pattern,
                token => UnsubscribeAsync(pattern, handleId, token),
                completion);
        }
        finally
        {
            _subscriptionGate.Release();
            registration?.Dispose();
        }
    }

    async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.KvSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        return DecodeSubscriptionResponse(response, "SUBSCRIBE", expectSubscriptionId: true)!.Value;
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership intentionally remains in the subscription table when the wire unsubscribe fails.")]
    async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<KvNotification>? registration = null;
        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var removedLocally = false;
            lock (_gate)
            {
                if (!_subscriptionsByPattern.TryGetValue(pattern, out var state) ||
                    !state.Writers.TryGetValue(handleId, out registration))
                {
                    return;
                }

                if (state.Writers.Count != 1)
                {
                    state.Writers.Remove(handleId);
                    removedLocally = true;
                }
            }

            if (removedLocally)
            {
                registration.Dispose();
                registration = null;
                return;
            }

            await UnsubscribeWireAsync(pattern, ct).ConfigureAwait(false);

            lock (_gate)
            {
                if (_subscriptionsByPattern.TryGetValue(pattern, out var state) &&
                    state.Writers.Remove(handleId, out registration))
                {
                    _subscriptionsByPattern.Remove(pattern);
                    _patternsBySubscriptionId.Remove(state.SubscriptionId);
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();
    }

    async Task UnsubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.KvUnsubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        DecodeSubscriptionResponse(response, "UNSUBSCRIBE", expectSubscriptionId: false);
    }

    static ulong? DecodeSubscriptionResponse(
        ReadOnlyMemory<byte> response,
        string operation,
        bool expectSubscriptionId)
    {
        var reader = KvWireHelpers.ReadSuccess(response, operation);

        var expectedBytes = expectSubscriptionId ? 8 : 0;
        if (reader.RemainingBytes != expectedBytes)
        {
            throw KvWireHelpers.InvalidResponse(operation, $"expected {expectedBytes} payload bytes, got {reader.RemainingBytes}");
        }

        return expectSubscriptionId ? reader.ReadU64() : null;
    }

    void EnsureNotificationHandlerInitialized()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_notificationHandlerInitialized)
                return;
            if (_registerNotificationHandler is null)
            {
                throw new InvalidOperationException("Notification handlers not configured for subscription support");
            }
            _notificationHandlerInitialized = true;
            _notificationRegistration = _registerNotificationHandler(MessageTypes.KvNotify, HandleNotification);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed broker notifications are dropped without disrupting the receive loop.")]
    void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var route = reader.ReadString();
            var mutationCount = reader.ReadU64();
            if (!reader.IsEof)
                return;
            SubscriptionRegistration<KvNotification>[] registrations;
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var state))
                    return;
                registrations = [.. state.Writers.Values];
            }

            var notification = new KvNotification(route, mutationCount);
            foreach (var writer in registrations)
            {
                if (!writer.Channel.Writer.TryWrite(notification))
                    _ = writer.FailOverflowAsync();
            }
        }
        catch
        {
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect restoration must best-effort roll back every already-restored subscription before preserving the original failure.")]
    async ValueTask RestoreSubscriptionsAsync(CancellationToken ct)
    {
        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            KeyValuePair<string, KvSubscriptionState>[] entries;
            lock (_gate)
            {
                entries = _subscriptionsByPattern.ToArray();
            }
            var restoredSubscriptions = new Dictionary<string, KvSubscriptionState>(StringComparer.Ordinal);
            var restoredPatternsById = new Dictionary<ulong, string>();
            try
            {
                foreach (var entry in entries)
                {
                    var subscriptionId = await SubscribeWireAsync(entry.Key, ct).ConfigureAwait(false);
                    restoredSubscriptions[entry.Key] = entry.Value.Clone(subscriptionId);
                    restoredPatternsById[subscriptionId] = entry.Key;
                }
            }
            catch
            {
                foreach (var pattern in restoredSubscriptions.Keys)
                {
                    try
                    {
                        await UnsubscribeWireAsync(pattern, CancellationToken.None).ConfigureAwait(false);
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
                _subscriptionsByPattern.Clear();
                _patternsBySubscriptionId.Clear();
                foreach (var entry in restoredSubscriptions)
                    _subscriptionsByPattern.Add(entry.Key, entry.Value);
                foreach (var entry in restoredPatternsById)
                    _patternsBySubscriptionId.Add(entry.Key, entry.Value);
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
            foreach (var state in _subscriptionsByPattern.Values)
            {
                foreach (var writer in state.Writers.Values)
                    writer.Dispose();
            }
            _subscriptionsByPattern.Clear();
            _patternsBySubscriptionId.Clear();
        }
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    sealed class KvSubscriptionState
    {
        internal KvSubscriptionState(ulong subscriptionId) => SubscriptionId = subscriptionId;
        internal ulong SubscriptionId { get; set; }
        internal Dictionary<long, SubscriptionRegistration<KvNotification>> Writers { get; } = [];

        internal KvSubscriptionState Clone(ulong subscriptionId)
        {
            var clone = new KvSubscriptionState(subscriptionId);
            foreach (var entry in Writers)
                clone.Writers.Add(entry.Key, entry.Value);
            return clone;
        }
    }
}
