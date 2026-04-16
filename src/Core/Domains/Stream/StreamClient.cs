using System.Runtime.CompilerServices;
using System.Threading.Channels;
using System.Text.Json;
using Cntryl.Fitz.Abstractions.Domains.Stream;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Stream;

public sealed class StreamClient : IStreamClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _gate = new();
    private readonly Dictionary<string, StreamSubscriptionState> _subscriptionsByPattern = new(StringComparer.Ordinal);
    private readonly Dictionary<ulong, string> _patternsBySubscriptionId = new();
    private IDisposable? _notificationRegistration;
    private bool _notificationHandlerInitialized;
    private long _nextHandleId;
    private readonly IDisposable? _reconnectRegistration;

    internal StreamClient(FitzConnection connection)
        : this(connection.RequestAsync, connection.RegisterNotificationHandler)
    {
        _reconnectRegistration = connection.OnReconnect(HandleReconnect);
    }

    public StreamClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
    }

    public async Task<IStreamSession> BeginAsync(string route, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        if (ingestMetadata.HasValue && ingestMetadata.Value.Length > 0)
        {
            writer.WriteU8(1);
            writer.WriteU32((uint)ingestMetadata.Value.Length);
            writer.WriteBytes(ingestMetadata.Value.Span);
        }
        else
        {
            writer.WriteU8(0);
        }

        var response = await _request(MessageTypes.StreamBegin, writer.Build(), ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new StreamException($"BEGIN failed with status {status}", "BEGIN_FAILED", status);
        }

        var hasSession = reader.IsEof ? (byte)0 : reader.ReadU8();
        if (hasSession != 1 || reader.RemainingBytes < 8)
        {
            throw new StreamException("BEGIN response missing session id", "MISSING_SESSION_ID");
        }

        return new StreamSession(_request, reader.ReadU64());
    }

    public async IAsyncEnumerable<StreamRecord> ReadAsync(
        string route,
        ulong startOffset,
        ulong limit = 100,
        ulong? maxBytes = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(startOffset);
        writer.WriteU64(limit);
        writer.WriteU8((byte)(maxBytes.HasValue ? 1 : 0));
        if (maxBytes.HasValue)
        {
            writer.WriteU64(maxBytes.Value);
        }

        var response = await _request(MessageTypes.StreamRead, writer.Build(), ct).ConfigureAwait(false);
        var (status, data) = ReadWrappedStreamResponse(response);
        if (status != 0)
        {
            throw new StreamException($"READ failed with status {status}", "READ_FAILED", status);
        }

        if (data.Length == 0)
        {
            yield break;
        }

        var records = ParseReadRecords(data);
        foreach (var record in records)
        {
            yield return record;
        }
    }

    public async Task<StreamRecord?> PeekAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);

        var response = await _request(MessageTypes.StreamLast, writer.Build(), ct).ConfigureAwait(false);
        var (status, data) = ReadWrappedStreamResponse(response);
        if (status != 0)
        {
            throw new StreamException($"LAST failed with status {status}", "LAST_FAILED", status);
        }

        if (data.Length == 0)
        {
            return null;
        }

        var reader = new BinaryBufferReader(data);
        var offset = reader.ReadU64();
        var bodyLength = reader.ReadU32();
        var body = reader.ReadBytes((int)bodyLength);
        return new StreamRecord(offset, body);
    }

    public async Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.StreamGetMetadata, writer.Build(), ct).ConfigureAwait(false);
        var (status, data) = ReadWrappedStreamResponse(response);
        if (status != 0)
        {
            throw new StreamException($"METADATA failed with status {status}", "METADATA_FAILED", status);
        }

        if (data.Length == 0)
        {
            return new StreamMetadata(0, 0, 0);
        }

        var inner = new BinaryBufferReader(data);
        return new StreamMetadata(inner.ReadU64(), inner.ReadU64(), inner.ReadU64());
    }

    public async Task<StreamSubscription> SubscribeAsync(
        string pattern,
        Func<StreamCommitEvent, CancellationToken, ValueTask> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        EnsureNotificationHandlerInitialized();

        var channel = Channel.CreateUnbounded<StreamCommitEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        var registration = new SubscriptionRegistration<StreamCommitEvent>(channel);
        var handleId = Interlocked.Increment(ref _nextHandleId);

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_subscriptionsByPattern.TryGetValue(pattern, out var existingSubscription))
            {
                existingSubscription.Registrations[handleId] = registration;
                var existingHandle = CreateSubscription(pattern, handleId, existingSubscription.SubscriptionId);
                SubscriptionPump.Start(registration, handler);
                return existingHandle;
            }

            var subscriptionId = await SubscribeWireAsync(pattern, ct).ConfigureAwait(false);
            var subscription = new StreamSubscriptionState(subscriptionId);
            subscription.Registrations[handleId] = registration;
            _subscriptionsByPattern[pattern] = subscription;
            _patternsBySubscriptionId[subscriptionId] = pattern;

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

    private StreamSubscription CreateSubscription(string pattern, long handleId, ulong subscriptionId)
    {
        return new StreamSubscription(
            subscriptionId,
            pattern,
            cancellationToken => UnsubscribeAsync(pattern, handleId, cancellationToken));
    }

    private async Task<ulong> SubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamSubscribe, writer.Build(), ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new StreamException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
        }

        if (reader.IsEof || reader.ReadU8() != 1 || reader.RemainingBytes < 8)
        {
            throw new StreamException("SUBSCRIBE response missing subscription id", "MISSING_SUB_ID");
        }

        return reader.ReadU64();
    }

    private async Task UnsubscribeWireAsync(string pattern, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        var response = await _request(MessageTypes.StreamUnsubscribe, writer.Build(), ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new StreamException($"UNSUBSCRIBE failed with status {status}", "UNSUBSCRIBE_FAILED", status);
        }
    }

    private async ValueTask UnsubscribeAsync(string pattern, long handleId, CancellationToken ct)
    {
        SubscriptionRegistration<StreamCommitEvent>? registration = null;
        bool shouldUnsubscribe = false;

        await _subscriptionGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
            {
                return;
            }

            if (!subscription.Registrations.Remove(handleId, out registration))
            {
                return;
            }

            if (subscription.Registrations.Count == 0)
            {
                _subscriptionsByPattern.Remove(pattern);
                _patternsBySubscriptionId.Remove(subscription.SubscriptionId);
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
            await UnsubscribeWireAsync(pattern, ct).ConfigureAwait(false);
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
        _notificationRegistration = _registerNotificationHandler(MessageTypes.StreamNotify, HandleNotification);
    }

    private void HandleNotification(byte[] payload)
    {
        try
        {
            var notifyReader = new BinaryBufferReader(payload);
            var subscriptionId = notifyReader.ReadU64();
            var route = notifyReader.ReadString();
            var bodyLength = notifyReader.ReadU32();
            var body = notifyReader.ReadBytes((int)bodyLength);

            SubscriptionRegistration<StreamCommitEvent>[] registrations;
            lock (_gate)
            {
                if (!_patternsBySubscriptionId.TryGetValue(subscriptionId, out var pattern) ||
                    !_subscriptionsByPattern.TryGetValue(pattern, out var subscription))
                {
                    return;
                }

                registrations = subscription.Registrations.Values.ToArray();
            }

            var notification = new StreamCommitEvent(route, TryParseCommitOffset(body));
            foreach (var registration in registrations)
            {
                registration.Channel.Writer.TryWrite(notification);
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

    private async ValueTask RestoreSubscriptionsAsync(CancellationToken cancellationToken)
    {
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<(string Pattern, StreamSubscriptionState Subscription)> snapshot;
            lock (_gate)
            {
                if (_subscriptionsByPattern.Count == 0)
                {
                    return;
                }

                snapshot = new List<(string Pattern, StreamSubscriptionState Subscription)>(_subscriptionsByPattern.Count);
                foreach (var entry in _subscriptionsByPattern)
                {
                    snapshot.Add((entry.Key, entry.Value.Clone()));
                }
            }

            var restoredSubscriptions = new Dictionary<string, StreamSubscriptionState>(StringComparer.Ordinal);
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

    private sealed class StreamSubscriptionState
    {
        public StreamSubscriptionState(ulong subscriptionId)
        {
            SubscriptionId = subscriptionId;
        }

        public ulong SubscriptionId { get; set; }

        public Dictionary<long, SubscriptionRegistration<StreamCommitEvent>> Registrations { get; } = new();

        public StreamSubscriptionState Clone()
        {
            var clone = new StreamSubscriptionState(SubscriptionId);
            foreach (var entry in Registrations)
            {
                clone.Registrations.Add(entry.Key, entry.Value);
            }

            return clone;
        }
    }

    private static ulong TryParseCommitOffset(byte[] payload)
    {
        try
        {
            using var document = JsonDocument.Parse(payload);
            if (document.RootElement.TryGetProperty("last_resource_offset", out var lastOffset))
            {
                return lastOffset.GetUInt64();
            }

            if (document.RootElement.TryGetProperty("first_resource_offset", out var firstOffset))
            {
                return firstOffset.GetUInt64();
            }
        }
        catch
        {
        }

        return 0;
    }

    private static (byte Status, byte[] Data) ReadWrappedStreamResponse(byte[] response)
    {
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            return (status, []);
        }

        if (!reader.IsEof)
        {
            var hasSession = reader.ReadU8();
            if (hasSession == 1 && reader.RemainingBytes >= 8)
            {
                _ = reader.ReadU64();
            }
        }

        return reader.IsEof ? ((byte)0, []) : ((byte)0, reader.ReadBytes((int)reader.ReadU32()));
    }

    private static List<StreamRecord> ParseReadRecords(byte[] data)
    {
        var fromCount = TryParseCountPrefixedRecords(data);
        if (fromCount.Count > 0)
        {
            return fromCount;
        }

        return ParseFlatRecords(data);
    }

    private static List<StreamRecord> TryParseCountPrefixedRecords(byte[] data)
    {
        var records = new List<StreamRecord>();
        if (data.Length < 4)
        {
            return records;
        }

        var reader = new BinaryBufferReader(data);
        uint count;
        try
        {
            count = reader.ReadU32();
        }
        catch
        {
            return records;
        }

        for (var index = 0U; index < count; index++)
        {
            try
            {
                var offset = reader.ReadU64();
                var bodyLength = reader.ReadU32();
                var body = reader.ReadBytes((int)bodyLength);
                records.Add(new StreamRecord(offset, body));
            }
            catch
            {
                records.Clear();
                return records;
            }
        }

        return reader.IsEof ? records : new List<StreamRecord>();
    }

    private static List<StreamRecord> ParseFlatRecords(byte[] data)
    {
        var records = new List<StreamRecord>();
        if (data.Length < 12)
        {
            return records;
        }

        var reader = new BinaryBufferReader(data);
        while (!reader.IsEof)
        {
            try
            {
                var offset = reader.ReadU64();
                var bodyLength = reader.ReadU32();
                var body = reader.ReadBytes((int)bodyLength);
                records.Add(new StreamRecord(offset, body));
            }
            catch
            {
                break;
            }
        }

        return records;
    }
}
