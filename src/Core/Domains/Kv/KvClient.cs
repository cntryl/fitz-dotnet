using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Kv;

public sealed class KvClient : IKvClient, IDisposable
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<Action, IDisposable>? _registerOnDisconnect;
    private readonly Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? _retryRequest;
    private readonly Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? _registerNotificationHandler;
    private readonly Func<Func<CancellationToken, ValueTask>, bool>? _dispatchAsyncHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, KvSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private IDisposable? _reconnectRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;

    internal KvClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.OnDisconnect,
            (operation, messageType, payload, cancellationToken) =>
                connection.ExecuteWithRetryAsync(
                    operation,
                    innerToken => connection.RequestAsync(messageType, payload, innerToken),
                    cancellationToken),
            connection.RegisterBorrowedNotificationHandler,
            connection.TryDispatchAsyncHandler)
    {
        _reconnectRegistration = connection.OnReconnect(RestoreSubscriptionsAsync);
    }

    public KvClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
        : this(async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)))
    {
    }

    internal KvClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<Action, IDisposable>? registerOnDisconnect = null,
        Func<RetryOperation, ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>? retryRequest = null,
        Func<ushort, Action<ReadOnlyMemory<byte>>, IDisposable>? registerNotificationHandler = null,
        Func<Func<CancellationToken, ValueTask>, bool>? dispatchAsyncHandler = null)
    {
        _request = request;
        _registerOnDisconnect = registerOnDisconnect;
        _retryRequest = retryRequest;
        _registerNotificationHandler = registerNotificationHandler;
        _dispatchAsyncHandler = dispatchAsyncHandler;
    }

    public async Task<IKvTransaction> BeginAsync(
        string route,
        KvDurability durability,
        KvMode mode = KvMode.ReadWrite,
        CancellationToken cancellationToken = default)
    {
        if (!RouteValidation.IsFixedRoute(route, "kv", 3))
        {
            throw new KvException($"route '{route}' must be kv://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU8((byte)mode);
        writer.WriteU8((byte)durability);

        var response = await _request(MessageTypes.KvBegin, writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
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

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The returned enumerable handle owns and disposes the callback registration.")]
    public async Task<KvSubscription> SubscribeAsync(
        string pattern,
        CancellationToken cancellationToken = default)
    {
        var buffer = new AsyncSubscriptionBuffer<KvNotification>(pattern);
        var registration = await SubscribeAsync(pattern, (notification, _) =>
        {
            buffer.Write(notification);
            return ValueTask.CompletedTask;
        }, cancellationToken).ConfigureAwait(false);
        return new KvSubscription(pattern, buffer.ReadAllAsync(CancellationToken.None), async token =>
        {
            buffer.Complete();
            await registration.UnsubscribeAsync(token).ConfigureAwait(false);
        });
    }

    internal async Task<KvSubscription> SubscribeAsync(
        string pattern,
        Func<KvNotification, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (!RouteValidation.IsRegistrationPattern(pattern, "kv", 3))
        {
            throw new KvException($"pattern '{pattern}' must use whole-segment wildcards and match a three-segment KV route", "INVALID_ROUTE");
        }
        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<KvNotification>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        SubscriptionRegistration<KvNotification>? registration = new(channel);
        var handleId = Interlocked.Increment(ref _nextHandleId);
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            KvSubscriptionState? state;
            lock (_gate)
            {
                _subscriptionsByPattern.TryGetValue(pattern, out state);
            }
            if (state is null)
            {
                var subscriptionId = await SubscribeWireAsync(pattern, cancellationToken).ConfigureAwait(false);
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
            registration = null;
            return new KvSubscription(pattern, token => UnsubscribeAsync(pattern, handleId, token));
        }
        finally
        {
            _subscriptionGate.Release();
            registration?.Dispose();
        }
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken cancellationToken)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.KvSubscribe, writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
        return DecodeSubscriptionResponse(response, "SUBSCRIBE", expectSubscriptionId: true)!.Value;
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken cancellationToken)
    {
        var removeWire = false;
        SubscriptionRegistration<KvNotification>? registration = null;
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_gate)
            {
                if (!_subscriptionsByPattern.TryGetValue(pattern, out var state) ||
                    !state.Writers.Remove(handleId, out registration))
                {
                    return;
                }
                if (state.Writers.Count == 0)
                {
                    _subscriptionsByPattern.Remove(pattern);
                    _patternsBySubscriptionId.Remove(state.SubscriptionId);
                    removeWire = true;
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
            registration?.Dispose();
        }
        if (!removeWire) return;
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);
        var response = await _request(MessageTypes.KvUnsubscribe, writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
        DecodeSubscriptionResponse(response, "UNSUBSCRIBE", expectSubscriptionId: false);
    }

    private static ulong? DecodeSubscriptionResponse(
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

    private void EnsureNotificationHandlerInitialized()
    {
        lock (_gate)
        {
            if (_notificationHandlerInitialized) return;
            if (_registerNotificationHandler is null)
            {
                throw new InvalidOperationException("Notification handlers not configured for subscription support");
            }
            _notificationHandlerInitialized = true;
            _notificationRegistration = _registerNotificationHandler(MessageTypes.KvNotify, HandleNotification);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Malformed broker notifications are dropped without disrupting the receive loop.")]
    private void HandleNotification(ReadOnlyMemory<byte> payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var route = reader.ReadString();
            var mutationCount = reader.ReadU64();
            if (!reader.IsEof) return;
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var state)) return;
                var notification = new KvNotification(route, mutationCount);
                foreach (var writer in state.Writers.Values)
                {
                    writer.Channel.Writer.TryWrite(notification);
                }
            }
        }
        catch
        {
        }
    }

    private async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            KeyValuePair<string, KvSubscriptionState>[] entries;
            lock (_gate)
            {
                entries = _subscriptionsByPattern.ToArray();
            }
            foreach (var entry in entries)
            {
                var subscriptionId = await SubscribeWireAsync(entry.Key, cancellationToken).ConfigureAwait(false);
                lock (_gate)
                {
                    _patternsBySubscriptionId.Remove(entry.Value.SubscriptionId);
                    entry.Value.SubscriptionId = subscriptionId;
                    _patternsBySubscriptionId[subscriptionId] = entry.Key;
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    public void Dispose()
    {
        _notificationRegistration?.Dispose();
        _reconnectRegistration?.Dispose();
        lock (_gate)
        {
            foreach (var state in _subscriptionsByPattern.Values)
            {
                foreach (var writer in state.Writers.Values) writer.Dispose();
            }
            _subscriptionsByPattern.Clear();
            _patternsBySubscriptionId.Clear();
        }
        _subscriptionGate.Dispose();
    }

    private sealed class KvSubscriptionState
    {
        internal KvSubscriptionState(ulong subscriptionId) => SubscriptionId = subscriptionId;
        internal ulong SubscriptionId { get; set; }
        internal Dictionary<long, SubscriptionRegistration<KvNotification>> Writers { get; } = new();
    }
}
