using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseClient : ILeaseClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly Action<ushort, Action<byte[]>>? _registerNotificationHandler;
    private readonly Action<ushort>? _unregisterNotificationHandler;

    internal LeaseClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.UnregisterNotificationHandler)
    {
    }

    public LeaseClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Action<ushort, Action<byte[]>>? registerNotificationHandler = null,
        Action<ushort>? unregisterNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _unregisterNotificationHandler = unregisterNotificationHandler;
    }

    public async Task<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU64(ttlSecs);
        var response = await _request(MessageTypes.LeaseAcquire, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"ACQUIRE failed with status {status}", "ACQUIRE_FAILED", status);
        }

        // Support both legacy [status][u64 token] and current
        // [status][u8 response_type][u64 token] response layouts.
        if (reader.RemainingBytes == 8)
        {
            return new LeaseHandle(_request, route, reader.ReadU64());
        }

        if (reader.RemainingBytes < 9)
        {
            throw new LeaseException("ACQUIRE response missing fencing token", "MISSING_TOKEN");
        }

        var responseType = reader.ReadU8();
        if (responseType >= 2)
        {
            throw new LeaseException($"ACQUIRE returned non-acquired response type {responseType}", "ACQUIRE_NOT_ACQUIRED");
        }

        return new LeaseHandle(_request, route, reader.ReadU64());
    }

    public async Task<LeaseInfo> QueryAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.LeaseQuery, writer.Build(), ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"QUERY failed with status {status}", "QUERY_FAILED", status);
        }

        var hasHolder = reader.ReadU8();
        if (hasHolder == 0)
        {
            return new LeaseInfo(false);
        }

        var owner = reader.ReadString();
        var ttlRemaining = reader.ReadU64();
        return new LeaseInfo(true, owner, ttlRemaining);
    }

    public async IAsyncEnumerable<LeaseChangeEvent> SubscribeAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_registerNotificationHandler == null || _unregisterNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        var channel = new SubscriptionChannel<LeaseChangeEvent>();

        // Register notification handler
        _registerNotificationHandler(MessageTypes.LeaseNotify, payload =>
        {
            try
            {
                var reader = new BinaryBufferReader(payload);
                var eventRoute = reader.ReadString();
                var isHeld = reader.ReadU8() == 1;
                var owner = reader.ReadU8() == 1 ? reader.ReadString() : null;
                var ttlRemaining = reader.ReadU8() == 1 ? (ulong?)reader.ReadU64() : null;
                channel.PostNotification(new LeaseChangeEvent(eventRoute, new LeaseStatus(isHeld, owner, ttlRemaining)));
            }
            catch
            {
                channel.Dispose();
            }
        });

        // Send subscribe request
        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        try
        {
            var response = await _request(MessageTypes.LeaseSubscribe, writer.Build(), ct);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new LeaseException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
            }
        }
        catch
        {
            _unregisterNotificationHandler(MessageTypes.LeaseNotify);
            throw;
        }

        // Yield events from the channel
        await foreach (var evt in channel.GetEnumerableAsync(ct))
        {
            yield return evt;
        }
    }
}