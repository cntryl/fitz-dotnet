using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Notice;

public sealed class NoticeClient : INoticeClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task> _send;
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>>? _request;
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, NoticeSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal NoticeClient(FitzConnection connection)
        : this(
            connection.SendAsync,
            connection.RequestAsync,
            connection.RegisterNotificationHandler)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public NoticeClient(
        Func<ushort, byte[], CancellationToken, Task> send,
        Func<ushort, byte[], CancellationToken, Task<byte[]>>? request = null,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
    {
        _send = send;
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
    }

    public Task PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);
        return _send(MessageTypes.NoticePublish, writer.Build(), ct);
    }

    public async Task<NoticeSubscription> SubscribeAsync(string pattern, Func<NoticeMessage, CancellationToken, ValueTask> handler, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<NoticeMessage>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        var registration = new NoticeHandlerRegistration(channel);

        var handleId = Interlocked.Increment(ref _nextHandleId);
        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_subscriptionsByPattern.TryGetValue(pattern, out var existingSubscription))
            {
                existingSubscription.Writers[handleId] = registration;
                var existingHandle = CreateSubscription(pattern, handleId, existingSubscription.SubscriptionId);
                StartHandlerPump(registration, handler);
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new NoticeSubscriptionState(subscriptionId);
            subscription.Writers[handleId] = registration;
            _subscriptionsByPattern[pattern] = subscription;
            _patternsBySubscriptionId[subscriptionId] = pattern;

            var handle = CreateSubscription(pattern, handleId, subscriptionId);
            StartHandlerPump(registration, handler);
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

    private NoticeSubscription CreateSubscription(
        string pattern,
        long handleId,
        ulong subscriptionId)
    {
        return new NoticeSubscription(
            subscriptionId,
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken));
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        ulong? subscriptionId = null;
        NoticeHandlerRegistration? registration = null;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
            {
                return;
            }

            if (!subscription.Writers.Remove(handleId, out registration))
            {
                return;
            }

            if (subscription.Writers.Count == 0)
            {
                subscriptionId = subscription.SubscriptionId;
                _subscriptionsByPattern.Remove(pattern);
                _patternsBySubscriptionId.Remove(subscription.SubscriptionId);
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }

        registration?.Dispose();

        if (subscriptionId.HasValue)
        {
            await UnsubscribeWireAsync(subscriptionId.Value, ct).ConfigureAwait(false);
        }
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice subscriptions");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.NoticeSubscribe, writer.Build(), ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new NoticeException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
        }

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new NoticeException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        return reader.ReadU64();
    }

    private async Task UnsubscribeWireAsync(ulong subscriptionId, CancellationToken ct)
    {
        if (_request is null)
        {
            throw new InvalidOperationException("Request support is required for notice unsubscription");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteU64(subscriptionId);

        var response = await _request(MessageTypes.NoticeUnsubscribe, writer.Build(), ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new NoticeException($"UNSUBSCRIBE failed with status {status}", "UNSUBSCRIBE_FAILED", status);
        }
    }

    private void EnsureNotificationHandlerInitialized()
    {
        if (_notificationHandlerInitialized)
        {
            return;
        }

        if (_registerNotificationHandler is null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        _notificationHandlerInitialized = true;
        _notificationRegistration = _registerNotificationHandler(MessageTypes.NoticeNotify, HandleNotification);
    }

    private void HandleNotification(byte[] payload)
    {
        try
        {
            var reader = new BinaryBufferReader(payload);
            var subscriptionId = reader.ReadU64();
            var route = reader.ReadString();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes((int)bodyLength);

            NoticeHandlerRegistration[] registrations;
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                registrations = subscription.Writers.Values.ToArray();
            }

            var message = new NoticeMessage(route, body);
            foreach (var registration in registrations)
            {
                registration.Channel.Writer.TryWrite(message);
            }
        }
        catch
        {
        }
    }

    private static void StartHandlerPump(NoticeHandlerRegistration registration, Func<NoticeMessage, CancellationToken, ValueTask> handler)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                while (await registration.Channel.Reader.WaitToReadAsync(registration.CancellationToken).ConfigureAwait(false))
                {
                    while (registration.Channel.Reader.TryRead(out var message))
                    {
                        try
                        {
                            await handler(message, registration.CancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch
            {
            }
        });
    }

    private async ValueTask HandleReconnect(CancellationToken cancellationToken)
    {
        await RestoreSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<(string Pattern, NoticeSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByPattern.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Pattern, NoticeSubscriptionState Subscription)>(_subscriptionsByPattern.Count);
                foreach (var entry in _subscriptionsByPattern)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, NoticeSubscriptionState>(StringComparer.Ordinal);
            var restoredPatternsById = new Dictionary<ulong, string>();

            foreach (var entry in snapshot)
            {
                var subscriptionId = await SubscribeWireAsync(entry.Pattern, cancellationToken).ConfigureAwait(false);
                entry.Subscription.SubscriptionId = subscriptionId;
                restoredSubscriptions[entry.Pattern] = entry.Subscription;
                restoredPatternsById[subscriptionId] = entry.Pattern;
            }

            lock (_gate)
            {
                _subscriptionsByPattern.Clear();
                _patternsBySubscriptionId.Clear();

                foreach (var entry in restoredSubscriptions)
                {
                    _subscriptionsByPattern[entry.Key] = entry.Value;
                }

                foreach (var entry in restoredPatternsById)
                {
                    _patternsBySubscriptionId[entry.Key] = entry.Value;
                }
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    private sealed class NoticeSubscriptionState
    {
        public NoticeSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; set; }

        public Dictionary<long, NoticeHandlerRegistration> Writers { get; } = new();

        public NoticeSubscriptionState Clone()
        {
            var clone = new NoticeSubscriptionState(SubscriptionId);
            foreach (var entry in Writers)
            {
                clone.Writers.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }

    private sealed class NoticeHandlerRegistration : IDisposable
    {
        private int _disposed;

        public NoticeHandlerRegistration(Channel<NoticeMessage> channel)
        {
            Channel = channel;
            CancellationSource = new CancellationTokenSource();
        }

        public Channel<NoticeMessage> Channel { get; }

        public CancellationToken CancellationToken => CancellationSource.Token;

        private CancellationTokenSource CancellationSource { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            CancellationSource.Cancel();
            Channel.Writer.TryComplete();
            CancellationSource.Dispose();
        }
    }
}
