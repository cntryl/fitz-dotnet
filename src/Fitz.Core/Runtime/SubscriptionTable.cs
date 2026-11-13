using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz.Runtime;

/// <summary>Registers a pattern with the broker and returns its subscription identifier.</summary>
/// <param name="pattern">The route or pattern to subscribe.</param>
/// <param name="ct">Cancellation token for the request.</param>
/// <returns>The broker-assigned subscription identifier.</returns>
delegate Task<ulong> SubscribeWire(string pattern, CancellationToken ct);

/// <summary>Withdraws a broker subscription.</summary>
/// <param name="pattern">The route or pattern that was subscribed.</param>
/// <param name="subscriptionId">The identifier the broker assigned to it.</param>
/// <param name="ct">Cancellation token for the request.</param>
/// <returns>A task that completes once the broker accepts the unsubscribe.</returns>
delegate Task UnsubscribeWire(string pattern, ulong subscriptionId, CancellationToken ct);

/// <summary>Decodes one NOTIFY frame for a domain.</summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <param name="payload">The NOTIFY payload.</param>
/// <returns>
/// The decoded notification, or <see langword="null"/> when the frame is malformed and must be dropped.
/// </returns>
/// <remarks>A decoder may also throw <see cref="FitzException"/> for a malformed frame.</remarks>
delegate DecodedNotification<TNotification>? NotificationDecoder<TNotification>(ReadOnlyMemory<byte> payload);

/// <summary>A decoded NOTIFY frame.</summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <param name="SubscriptionId">The broker subscription the frame was delivered for.</param>
/// <param name="Route">The concrete route the notification describes.</param>
/// <param name="Value">The notification delivered to handlers.</param>
readonly record struct DecodedNotification<TNotification>(ulong SubscriptionId, string Route, TNotification Value);

/// <summary>Builds a domain's public subscription handle around a buffered notification stream.</summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <typeparam name="TSubscription">The domain's subscription handle type.</typeparam>
delegate TSubscription BufferedSubscriptionFactory<TNotification, TSubscription>(
    string pattern,
    IAsyncEnumerable<TNotification> notifications,
    Func<CancellationToken, ValueTask> unsubscribe,
    Task completion,
    TimeProvider time);

/// <summary>
/// The subscription bookkeeping every notification domain shares: one broker subscription per
/// pattern, any number of local handles on it, routing of NOTIFY frames to those handles,
/// restoration after reconnect, and ordered shutdown.
/// </summary>
/// <typeparam name="TNotification">The domain's notification type.</typeparam>
/// <remarks>
/// Domains own only what differs between them: pattern validation, the wire encoding of
/// SUBSCRIBE and UNSUBSCRIBE, and the NOTIFY decoder. Subscription changes are serialized by one
/// gate, and the broker subscription is withdrawn only when the last local handle leaves. A
/// failed wire unsubscribe keeps the handle registered so the caller can retry.
/// </remarks>
sealed class SubscriptionTable<TNotification> : IAsyncDisposable
{
    readonly object _owner;
    readonly string _domain;
    readonly ushort _notifyMessageType;
    readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    readonly AsyncHandlerDispatch? _dispatchAsyncHandler;
    readonly TimeProvider _time;
    readonly SubscribeWire _subscribe;
    readonly UnsubscribeWire _unsubscribe;
    readonly NotificationDecoder<TNotification> _decode;
    readonly NotificationActivationBuffer _activationBuffer;
    readonly SemaphoreSlim _changeGate = new(1, 1);
    readonly CancellationTokenSource _closing = new();
    readonly CancellationToken _closingToken;
    readonly object _gate = new();
    readonly Dictionary<string, PatternSubscription> _byPattern = [with(StringComparer.Ordinal)];
    readonly Dictionary<ulong, string> _patternById = [];
    IDisposable? _notificationRegistration;
    long _nextHandleId;
    int _disposed;

    /// <summary>Creates the table.</summary>
    /// <param name="owner">The domain client, named in <see cref="ObjectDisposedException"/>.</param>
    /// <param name="domain">Domain name used in diagnostics and overflow errors.</param>
    /// <param name="notifyMessageType">The domain's NOTIFY message type.</param>
    /// <param name="registerNotificationHandler">Connection hook for NOTIFY frames, or <see langword="null"/> when subscriptions are unsupported.</param>
    /// <param name="dispatchAsyncHandler">Shared callback dispatcher, or <see langword="null"/> to run handlers inline.</param>
    /// <param name="bufferCapacity">Per-handle notification buffer size.</param>
    /// <param name="subscribe">Encodes SUBSCRIBE and returns the subscription identifier.</param>
    /// <param name="unsubscribe">Encodes UNSUBSCRIBE.</param>
    /// <param name="decode">Decodes NOTIFY frames.</param>
    /// <param name="timeProvider">The clock for overflow cleanup budgets; the system clock by default.</param>
    internal SubscriptionTable(
        object owner,
        string domain,
        ushort notifyMessageType,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler,
        AsyncHandlerDispatch? dispatchAsyncHandler,
        int bufferCapacity,
        SubscribeWire subscribe,
        UnsubscribeWire unsubscribe,
        NotificationDecoder<TNotification> decode,
        TimeProvider? timeProvider = null)
    {
        _owner = owner;
        _time = timeProvider ?? TimeProvider.System;
        _domain = domain;
        _notifyMessageType = notifyMessageType;
        _registerNotificationHandler = registerNotificationHandler;
        _dispatchAsyncHandler = dispatchAsyncHandler;
        BufferCapacity = bufferCapacity;
        _subscribe = subscribe;
        _unsubscribe = unsubscribe;
        _decode = decode;
        _activationBuffer = new NotificationActivationBuffer(bufferCapacity);
        _closingToken = _closing.Token;
    }

    /// <summary>Per-handle notification buffer size.</summary>
    internal int BufferCapacity { get; }

    /// <summary>
    /// Restores subscriptions when <paramref name="connection"/> reconnects and fails them when it
    /// cannot deliver notifications.
    /// </summary>
    /// <param name="connection">The connection the domain client runs on.</param>
    /// <param name="onReconnect">
    /// Replaces the default reconnect handling (<see cref="RestoreAsync"/>) for domains that do more on reconnect.
    /// </param>
    /// <returns>A binding that detaches both hooks when disposed.</returns>
    internal IDisposable AttachTo(FitzConnection connection, Func<CancellationToken, ValueTask>? onReconnect = null) =>
        new ConnectionBinding(connection, onReconnect ?? RestoreAsync, Fail);

    /// <summary>
    /// Subscribes and exposes notifications as an asynchronous stream wrapped in the domain's handle.
    /// </summary>
    /// <typeparam name="TSubscription">The domain's subscription handle type.</typeparam>
    /// <param name="pattern">A route or pattern the domain has already validated.</param>
    /// <param name="create">Builds the domain handle.</param>
    /// <param name="preDispatch">Optional receive-path hook; see <see cref="SubscribeAsync"/>.</param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>The domain handle.</returns>
    internal async Task<TSubscription> SubscribeBufferedAsync<TSubscription>(
        string pattern,
        BufferedSubscriptionFactory<TNotification, TSubscription> create,
        Action<TNotification>? preDispatch,
        CancellationToken ct)
    {
        var buffer = new AsyncSubscriptionBuffer<TNotification>(pattern, BufferCapacity);
        var binding = await SubscribeAsync(pattern, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, preDispatch, ct).ConfigureAwait(false);
        buffer.ObserveCompletion(binding.Completion);
        return create(pattern, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            await binding.Unsubscribe(token).ConfigureAwait(false);
            buffer.Complete();
        }, binding.Completion, _time);
    }

    /// <summary>The clock that bounds subscription cleanup, for handles built outside the table.</summary>
    internal TimeProvider Time => _time;

    /// <summary>Subscribes a callback to a pattern.</summary>
    /// <param name="pattern">A route or pattern the domain has already validated.</param>
    /// <param name="handler">Callback invoked for each notification.</param>
    /// <param name="preDispatch">
    /// Optional synchronous hook run on the receive path before the notification is queued. A
    /// <see cref="FitzException"/> it throws faults only this handle.
    /// </param>
    /// <param name="ct">Cancellation token for the subscribe request.</param>
    /// <returns>The handle's unsubscribe callback and completion signal.</returns>
    internal async Task<SubscriptionBinding> SubscribeAsync(
        string pattern,
        Func<TNotification, CancellationToken, ValueTask> handler,
        Action<TNotification>? preDispatch,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(handler);
        EnsureNotificationHandlerRegistered();

        var handleId = Interlocked.Increment(ref _nextHandleId);
        SubscriptionRegistration<TNotification>? registration = new(
            SubscriptionRegistration<TNotification>.CreateChannel(BufferCapacity),
            _domain,
            pattern,
            token => UnsubscribeAsync(pattern, handleId, token),
            preDispatch,
            _time);
        var binding = new SubscriptionBinding(token => UnsubscribeAsync(pattern, handleId, token), registration.Completion);
        var gateEntered = false;
        try
        {
            await _changeGate.EnterAsync(_owner, _closingToken, ct).ConfigureAwait(false);
            gateEntered = true;
            lock (_gate)
            {
                ThrowIfDisposed();
                if (_byPattern.TryGetValue(pattern, out var existing))
                {
                    // Sibling handles share the broker subscription already in place.
                    existing.Handles[handleId] = registration;
                    SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
                    registration = null;
                    return binding;
                }
            }

            using var activation = _activationBuffer.Begin(pattern);
            var subscriptionId = await _subscribe(pattern, ct).ConfigureAwait(false);
            var subscription = new PatternSubscription(subscriptionId);
            subscription.Handles[handleId] = registration;
            lock (_gate)
            {
                ThrowIfDisposed();
                _byPattern[pattern] = subscription;
                _patternById[subscriptionId] = pattern;
                if (_activationBuffer.Replay(subscriptionId, HandleNotification) is { } activationError)
                    _ = registration.FailAsync(activationError);
            }

            SubscriptionPump.Start(registration, handler, _dispatchAsyncHandler);
            registration = null;
            return binding;
        }
        finally
        {
            if (gateEntered)
                _changeGate.Release();
            registration?.Dispose();
        }
    }

    /// <summary>
    /// Runs a broker request that drops every subscription at once, then ends every local handle.
    /// </summary>
    /// <param name="request">The domain's wire request.</param>
    /// <param name="ct">Cancellation token for the request.</param>
    /// <returns>A task that completes once the broker accepts the request.</returns>
    internal async Task RemoveAllAsync(Func<CancellationToken, Task> request, CancellationToken ct)
    {
        await _changeGate.EnterAsync(_owner, _closingToken, ct).ConfigureAwait(false);
        try
        {
            await request(ct).ConfigureAwait(false);
            foreach (var registration in DetachAll())
                registration.Dispose();
        }
        finally
        {
            _changeGate.Release();
        }
    }

    /// <summary>
    /// Re-establishes every broker subscription on a new session. On failure, rolls back the
    /// subscriptions it re-created and keeps the previous state so a later reconnect can retry.
    /// </summary>
    /// <param name="ct">Cancellation token for the restore requests.</param>
    /// <returns>A task that completes once every subscription is restored.</returns>
    internal async ValueTask RestoreAsync(CancellationToken ct)
    {
        if (!await _changeGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            return;

        try
        {
            KeyValuePair<string, PatternSubscription>[] snapshot;
            lock (_gate)
            {
                if (_byPattern.Count == 0)
                    return;
                snapshot = [.. _byPattern];
            }

            var restored = new List<(string Pattern, PatternSubscription Subscription)>(snapshot.Length);
            try
            {
                foreach (var (pattern, subscription) in snapshot)
                {
                    var subscriptionId = await _subscribe(pattern, ct).ConfigureAwait(false);
                    restored.Add((pattern, subscription.WithId(subscriptionId)));
                }
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
                foreach (var (pattern, subscription) in restored)
                {
                    try
                    {
                        await _unsubscribe(pattern, subscription.SubscriptionId, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception cleanupError) when (ExceptionPolicy.IsRecoverable(cleanupError))
                    {
                        // Best effort rollback; preserve the original restore failure.
                    }
                }

                throw;
            }

            lock (_gate)
            {
                _byPattern.Clear();
                _patternById.Clear();
                foreach (var (pattern, subscription) in restored)
                {
                    _byPattern[pattern] = subscription;
                    _patternById[subscription.SubscriptionId] = pattern;
                }
            }
        }
        finally
        {
            _changeGate.Release();
        }
    }

    /// <summary>
    /// Ends every handle with <paramref name="error"/> after the connection could not deliver
    /// notifications. The handles cannot be restored; callers must subscribe again.
    /// </summary>
    /// <param name="error">The delivery failure.</param>
    internal void Fail(Exception error)
    {
        lock (_gate)
        {
            _activationBuffer.Fail(error);
            foreach (var registration in DetachAllUnsafe())
                _ = registration.FailAsync(error);
        }
    }

    /// <summary>
    /// Ends every local handle and releases resources. Waits for an in-progress subscription change;
    /// queued and later changes fail with <see cref="ObjectDisposedException"/>, and unsubscribes
    /// and restoration become no-ops.
    /// </summary>
    /// <returns>A task that completes once shutdown finishes.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _closing.CancelAsync().ConfigureAwait(false);
        _activationBuffer.Dispose();
        _notificationRegistration?.Dispose();
        foreach (var registration in DetachAll())
            registration.Dispose();

        await _changeGate.DrainAndDisposeAsync().ConfigureAwait(false);
        _closing.Dispose();
    }

    async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<TNotification>? removed = null;
        if (!await _changeGate.TryEnterAsync(_closingToken, ct).ConfigureAwait(false))
            return;

        try
        {
            ulong subscriptionId;
            lock (_gate)
            {
                if (!_byPattern.TryGetValue(pattern, out var subscription) ||
                    !subscription.Handles.ContainsKey(handleId))
                {
                    return;
                }

                // Sibling handles keep the broker subscription; only the last one withdraws it.
                if (subscription.Handles.Count != 1)
                {
                    subscription.Handles.Remove(handleId, out removed);
                    return;
                }

                subscriptionId = subscription.SubscriptionId;
            }

            // A failed wire unsubscribe leaves the handle registered, so the caller can retry.
            await _unsubscribe(pattern, subscriptionId, ct).ConfigureAwait(false);

            lock (_gate)
            {
                if (_byPattern.TryGetValue(pattern, out var subscription) &&
                    subscription.Handles.Remove(handleId, out removed))
                {
                    _byPattern.Remove(pattern);
                    _patternById.Remove(subscription.SubscriptionId);
                }
            }
        }
        finally
        {
            _changeGate.Release();
            removed?.Dispose();
        }
    }

    void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        DecodedNotification<TNotification> decoded;
        try
        {
            if (_decode(payload) is not { } result)
            {
                FitzDiagnostics.RecordDroppedNotification(_domain, "malformed");
                return;
            }

            decoded = result;
        }
        catch (FitzException)
        {
            FitzDiagnostics.RecordDroppedNotification(_domain, "malformed");
            return;
        }

        SubscriptionRegistration<TNotification>[] registrations;
        lock (_gate)
        {
            if (!_patternById.TryGetValue(decoded.SubscriptionId, out var pattern) ||
                !_byPattern.TryGetValue(pattern, out var subscription))
            {
                // The subscribe response may still be in flight; replayed once it is recorded.
                _activationBuffer.Buffer(decoded.SubscriptionId, decoded.Route, payload);
                return;
            }

            registrations = [.. subscription.Handles.Values];
        }

        foreach (var registration in registrations)
        {
            try
            {
                registration.PreDispatch?.Invoke(decoded.Value);
            }
            catch (FitzException error)
            {
                _ = registration.FailAsync(error);
                continue;
            }

            if (!registration.Channel.Writer.TryWrite(decoded.Value))
                _ = registration.FailOverflowAsync();
        }
    }

    void EnsureNotificationHandlerRegistered()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_notificationRegistration is not null)
                return;
            if (_registerNotificationHandler is null)
                throw new InvalidOperationException("Notification handlers not configured for subscription support");
            _notificationRegistration = _registerNotificationHandler(_notifyMessageType, HandleNotification);
        }
    }

    SubscriptionRegistration<TNotification>[] DetachAll()
    {
        lock (_gate)
        {
            return DetachAllUnsafe();
        }
    }

    // Callers hold _gate.
    SubscriptionRegistration<TNotification>[] DetachAllUnsafe()
    {
        var registrations = _byPattern.Values.SelectMany(static subscription => subscription.Handles.Values).ToArray();
        _byPattern.Clear();
        _patternById.Clear();
        return registrations;
    }

    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, _owner);

    sealed class ConnectionBinding : IDisposable
    {
        readonly FitzConnection _connection;
        readonly Action<Exception> _onDeliveryFailed;
        readonly IDisposable _reconnect;
        int _disposed;

        internal ConnectionBinding(FitzConnection connection, Func<CancellationToken, ValueTask> onReconnect, Action<Exception> onDeliveryFailed)
        {
            _connection = connection;
            _onDeliveryFailed = onDeliveryFailed;
            connection.NotificationDeliveryFailed += onDeliveryFailed;
            _reconnect = connection.OnReconnect(onReconnect);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            _connection.NotificationDeliveryFailed -= _onDeliveryFailed;
            _reconnect.Dispose();
        }
    }

    /// <summary>One broker subscription and the local handles sharing it.</summary>
    sealed class PatternSubscription(ulong subscriptionId)
    {
        internal ulong SubscriptionId { get; } = subscriptionId;

        internal Dictionary<long, SubscriptionRegistration<TNotification>> Handles { get; } = [];

        // The same handles, re-bound to the identifier a new session assigned.
        internal PatternSubscription WithId(ulong subscriptionId)
        {
            var copy = new PatternSubscription(subscriptionId);
            foreach (var (handleId, registration) in Handles)
                copy.Handles.Add(handleId, registration);
            return copy;
        }
    }
}
