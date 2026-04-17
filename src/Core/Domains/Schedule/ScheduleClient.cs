using System.Collections.Generic;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Schedule;

public sealed class ScheduleClient : IScheduleClient
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, ScheduleSubscriptionState> _subscriptionsByRoute = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _routesBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal ScheduleClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public ScheduleClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
        : this(async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)), registerNotificationHandler)
    {
    }

    internal ScheduleClient(
        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
    }

    public async ValueTask<string?> CreateAsync(string route, string cron, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ValidateScheduleRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(cron);
        writer.WriteU32((uint)payload.Length);
        writer.WriteBytes(payload.Span);
        var data = await AssertSuccessAsync(MessageTypes.ScheduleCreate, writer.WrittenMemory, "CREATE", ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(data);
        if (!reader.IsEof && reader.ReadU8() == 1)
        {
            var createdRoute = reader.ReadString();
            if (!reader.IsEof)
            {
                throw new ScheduleException("CREATE response has trailing bytes", "CREATE_INVALID_RESPONSE");
            }

            return createdRoute;
        }

        if (!reader.IsEof)
        {
            throw new ScheduleException("CREATE response has trailing bytes", "CREATE_INVALID_RESPONSE");
        }

        return route;
    }

    public async ValueTask CancelAsync(string route, CancellationToken ct = default)
    {
        ValidateScheduleRoute(route);

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var data = await AssertSuccessAsync(MessageTypes.ScheduleCancel, writer.WrittenMemory, "CANCEL", ct).ConfigureAwait(false);
        if (!data.IsEmpty)
        {
            throw new ScheduleException("CANCEL response has trailing bytes", "CANCEL_INVALID_RESPONSE");
        }
    }

    public async Task<(ScheduleEntry[] Entries, ulong TotalCount)> ListAsync(ulong offset = 0, ulong limit = 0, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(offset > 0 ? (byte)1 : (byte)0);
        if (offset > 0)
        {
            writer.WriteU64(offset);
        }

        writer.WriteU8(limit > 0 ? (byte)1 : (byte)0);
        if (limit > 0)
        {
            writer.WriteU64(limit);
        }

        var data = await AssertSuccessAsync(MessageTypes.ScheduleList, writer.WrittenMemory, "LIST", ct).ConfigureAwait(false);
        if (data.Length == 0)
        {
            return ([], 0);
        }

        var reader = new BinaryBufferReader(data);
        var totalCount = reader.ReadU64();
        var entries = new List<ScheduleEntry>();

        while (!reader.IsEof)
        {
            var hasEntry = reader.ReadU8();
            if (hasEntry == 0)
            {
                break;
            }

            var route = reader.ReadString();
            var cron = reader.ReadString();
            var payloadLength = reader.ReadU32();
            var payload = reader.ReadBytes((int)payloadLength);
            entries.Add(new ScheduleEntry(route, route, cron, payload));
        }

        if (!reader.IsEof)
        {
            throw new ScheduleException("LIST response has trailing bytes", "LIST_INVALID_RESPONSE");
        }

        return (entries.ToArray(), totalCount);
    }

    public async Task<ScheduleSubscription> SubscribeAsync(
        string pattern,
        Func<ScheduleNotification, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ValidateScheduleRoute(pattern);
        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<ScheduleNotification>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        var registration = new SubscriptionRegistration<ScheduleNotification>(channel);

        var handleId = Interlocked.Increment(ref _nextHandleId);

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_subscriptionsByRoute.TryGetValue(pattern, out var existingSubscription))
            {
                existingSubscription.Writers[handleId] = registration;
                var existingHandle = CreateSubscription(pattern, handleId, existingSubscription.SubscriptionId);
                SubscriptionPump.Start(registration, handler);
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new ScheduleSubscriptionState(subscriptionId);
            subscription.Writers[handleId] = registration;
            _subscriptionsByRoute[pattern] = subscription;
            _routesBySubscriptionId[subscriptionId] = pattern;

            var handle = CreateSubscription(pattern, handleId, subscriptionId);
            SubscriptionPump.Start(registration, handler);
            return handle;
        }
        catch
        {
            registration.Dispose();
            throw;
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    private ScheduleSubscription CreateSubscription(
        string route,
        long handleId,
        ulong subscriptionId)
    {
        return new ScheduleSubscription(
            subscriptionId,
            route,
            cancellationToken => UnsubscribeAsync(route, handleId, cancellationToken));
    }

    private async ValueTask UnsubscribeAsync(string route, long handleId, CancellationToken ct)
    {
        bool shouldUnsubscribe = false;
        SubscriptionRegistration<ScheduleNotification>? registration = null;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_subscriptionsByRoute.TryGetValue(route, out var subscription))
            {
                return;
            }

            if (!subscription.Writers.Remove(handleId, out registration))
            {
                return;
            }

            if (subscription.Writers.Count == 0)
            {
                _subscriptionsByRoute.Remove(route);
                _routesBySubscriptionId.Remove(subscription.SubscriptionId);
                shouldUnsubscribe = true;
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();

        if (shouldUnsubscribe)
        {
            await UnsubscribeWireAsync(route, ct).ConfigureAwait(false);
        }
    }

    private async Task<ulong> SubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.ScheduleSubscribe, writer.WrittenMemory, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new ScheduleException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
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

    private async Task UnsubscribeWireAsync(string route, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var data = await AssertSuccessAsync(MessageTypes.ScheduleUnsubscribe, writer.WrittenMemory, "UNSUBSCRIBE", ct).ConfigureAwait(false);
        _ = data;
    }

    private void EnsureNotificationHandlerInitialized()
    {
        if (_notificationHandlerInitialized)
        {
            return;
        }

        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        _notificationHandlerInitialized = true;
        _notificationRegistration = _registerNotificationHandler(MessageTypes.ScheduleNotify, HandleNotification);
    }

    private void HandleNotification(byte[] payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes((int)bodyLength);

            lock (_gate)
            {
                if (!_routesBySubscriptionId.TryGetValue(subscriptionId, out var route) ||
                    !_subscriptionsByRoute.TryGetValue(route, out var subscription))
                {
                    return;
                }

                var notification = new ScheduleNotification(body);
                foreach (var registration in subscription.Writers.Values)
                {
                    registration.Channel.Writer.TryWrite(notification);
                }
            }
        }
        catch
        {
        }
    }

    private async ValueTask HandleReconnect(CancellationToken cancellationToken)
    {
        await RestoreSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateScheduleRoute(string route)
    {
        if (RouteValidation.TryValidateFixedRoute(route, "schedule", 4, out var failure))
        {
            return;
        }

        throw CreateRouteException(route, failure);
    }

    private static ScheduleException CreateRouteException(string route, RouteValidationFailure failure)
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

    private async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
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

            foreach (var entry in snapshot)
            {
                var subscriptionId = await SubscribeWireAsync(entry.Route, cancellationToken).ConfigureAwait(false);
                entry.Subscription.SubscriptionId = subscriptionId;
                restoredSubscriptions[entry.Route] = entry.Subscription;
                restoredRoutesById[subscriptionId] = entry.Route;
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

    private sealed class ScheduleSubscriptionState
    {
        public ScheduleSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; set; }

        public Dictionary<long, SubscriptionRegistration<ScheduleNotification>> Writers { get; } = new();

        public ScheduleSubscriptionState Clone()
        {
            var clone = new ScheduleSubscriptionState(SubscriptionId);
            foreach (var entry in Writers)
            {
                clone.Writers.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }

    private async ValueTask<ReadOnlyMemory<byte>> AssertSuccessAsync(ushort messageType, ReadOnlyMemory<byte> payload, string operation, CancellationToken ct)
    {
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new ScheduleException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        return reader.IsEof ? ReadOnlyMemory<byte>.Empty : response.Slice(1);
    }
}
