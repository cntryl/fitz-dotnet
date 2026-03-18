using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Schedule;

public sealed class ScheduleClient : IScheduleClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly Action<ushort, Action<byte[]>>? _registerNotificationHandler;
    private readonly Action<ushort>? _unregisterNotificationHandler;

    internal ScheduleClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.UnregisterNotificationHandler)
    {
    }

    public ScheduleClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Action<ushort, Action<byte[]>>? registerNotificationHandler = null,
        Action<ushort>? unregisterNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _unregisterNotificationHandler = unregisterNotificationHandler;
    }

    public async Task<string?> CreateAsync(string route, string cron, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(cron);
        writer.WriteU32((uint)payload.Length);
        writer.WriteBytes(payload.Span);
        var data = await AssertSuccessAsync(MessageTypes.ScheduleCreate, writer.Build(), "CREATE", ct);
        var reader = new BinaryBufferReader(data);
        if (!reader.IsEof && reader.ReadU8() == 1)
        {
            return reader.ReadString();
        }

        // Route is the canonical identity when server omits explicit schedule id.
        return route;
    }

    public async Task CancelAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        _ = await AssertSuccessAsync(MessageTypes.ScheduleCancel, writer.Build(), "CANCEL", ct);
    }

    public async IAsyncEnumerable<ScheduleExecutionEvent> SubscribeAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_registerNotificationHandler == null || _unregisterNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        var channel = new SubscriptionChannel<ScheduleExecutionEvent>();

        // Register notification handler
        _registerNotificationHandler(MessageTypes.ScheduleNotify, payload =>
        {
            try
            {
                var reader = new BinaryBufferReader(payload);
                var scheduleId = reader.ReadString();
                var route = reader.ReadString();
                var executedAtMs = reader.ReadU64();
                channel.PostNotification(new ScheduleExecutionEvent(scheduleId, route, executedAtMs));
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
            var response = await _request(MessageTypes.ScheduleSubscribe, writer.Build(), ct);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new ScheduleException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
            }
        }
        catch
        {
            _unregisterNotificationHandler(MessageTypes.ScheduleNotify);
            throw;
        }

        // Yield events from the channel
        await foreach (var evt in channel.GetEnumerableAsync(ct))
        {
            yield return evt;
        }
    }

    private async Task<byte[]> AssertSuccessAsync(ushort messageType, byte[] payload, string operation, CancellationToken ct)
    {
        var response = await _request(messageType, payload, ct);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new ScheduleException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        return reader.IsEof ? [] : reader.ReadBytes(reader.RemainingBytes);
    }
}