using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Queue;

public sealed class QueueClient : IQueueClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly Action<ushort, Action<byte[]>>? _registerNotificationHandler;
    private readonly Action<ushort>? _unregisterNotificationHandler;

    internal QueueClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.UnregisterNotificationHandler)
    {
    }

    public QueueClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Action<ushort, Action<byte[]>>? registerNotificationHandler = null,
        Action<ushort>? unregisterNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _unregisterNotificationHandler = unregisterNotificationHandler;
    }

    public async Task<ulong> EnqueueAsync(
        string route,
        ReadOnlyMemory<byte> body,
        int? delayMs = null,
        CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);

        var delaySeconds = (delayMs ?? 0) / 1000;
        writer.WriteU8((byte)(delaySeconds > 0 ? 1 : 0));
        if (delaySeconds > 0)
        {
            writer.WriteU64((ulong)delaySeconds);
        }

        var response = await _request(MessageTypes.QueueEnqueue, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"ENQUEUE failed with status {status}", "ENQUEUE_FAILED", status);
        }

        return reader.IsEof ? 0UL : reader.ReadU64();
    }

    public async Task<IQueueReservedItem[]> ReserveAsync(
        string route,
        ulong leaseSeconds,
        int batchSize = 1,
        int? waitSeconds = null,
        CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU64(leaseSeconds);

        var normalizedBatchSize = batchSize > 0 ? batchSize : 1;
        writer.WriteU8((byte)(normalizedBatchSize > 0 ? 1 : 0));
        if (normalizedBatchSize > 0)
        {
            writer.WriteU32((uint)normalizedBatchSize);
        }

        writer.WriteU8((byte)(waitSeconds.HasValue && waitSeconds.Value > 0 ? 1 : 0));
        if (waitSeconds.HasValue && waitSeconds.Value > 0)
        {
            writer.WriteU64((ulong)waitSeconds.Value);
        }

        var response = await _request(MessageTypes.QueueReserve, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"RESERVE failed with status {status}", "RESERVE_FAILED", status);
        }

        var count = reader.IsEof ? 0U : reader.ReadU32();
        var items = new IQueueReservedItem[count];
        for (var i = 0; i < count; i++)
        {
            var itemId = reader.ReadU64();
            var itemToken = reader.ReadU64();
            var bodyLength = reader.ReadU32();
            var body = reader.ReadBytes((int)bodyLength);

            items[i] = new QueueReservedItem(
                route,
                itemId,
                itemToken,
                body,
                1,
                _request
            );
        }

        return items;
    }

    public async IAsyncEnumerable<QueueAvailabilityEvent> SubscribeAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_registerNotificationHandler == null || _unregisterNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        var channel = new SubscriptionChannel<QueueAvailabilityEvent>();

        // Register notification handler
        _registerNotificationHandler(MessageTypes.QueueNotify, payload =>
        {
            try
            {
                var reader = new BinaryBufferReader(payload);
                var eventRoute = reader.ReadString();
                var messageCount = reader.ReadU64();
                channel.PostNotification(new QueueAvailabilityEvent(eventRoute, messageCount));
            }
            catch
            {
                // Silently ignore parsing errors in notifications
                channel.Dispose();
            }
        });

        // Send subscribe request to server
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        try
        {
            var response = await _request(MessageTypes.QueueSubscribe, writer.Build(), ct);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                _unregisterNotificationHandler(MessageTypes.QueueNotify);
                throw new QueueException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
            }
        }
        catch
        {
            _unregisterNotificationHandler(MessageTypes.QueueNotify);
            throw;
        }

        // Yield events from the channel
        await foreach (var evt in channel.GetEnumerableAsync(ct))
        {
            yield return evt;
        }
    }
}