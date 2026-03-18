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
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;

    internal ScheduleClient(FitzConnection connection)
        : this(
            connection.RequestAsync,
            connection.RegisterNotificationHandler)
    {
    }

    public ScheduleClient(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        Func<ushort, Action<byte[]>, IDisposable>? registerNotificationHandler = null)
    {
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
    }

    public async Task<string?> CreateAsync(string route, string cron, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(cron);
        writer.WriteU32((uint)payload.Length);
        writer.WriteBytes(payload.Span);
        var data = await AssertSuccessAsync(MessageTypes.ScheduleCreate, writer.Build(), "CREATE", ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(data);
        if (!reader.IsEof && reader.ReadU8() == 1)
        {
            return reader.ReadString();
        }

        return route;
    }

    public async Task CancelAsync(string route, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        _ = await AssertSuccessAsync(MessageTypes.ScheduleCancel, writer.Build(), "CANCEL", ct).ConfigureAwait(false);
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

        var data = await AssertSuccessAsync(MessageTypes.ScheduleList, writer.Build(), "LIST", ct).ConfigureAwait(false);
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

        return (entries.ToArray(), totalCount);
    }

    public async IAsyncEnumerable<ScheduleExecutionEvent> SubscribeAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        var channel = new SubscriptionChannel<ScheduleExecutionEvent>();
        var registration = _registerNotificationHandler(MessageTypes.ScheduleNotify, payload =>
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

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        try
        {
            var response = await _request(MessageTypes.ScheduleSubscribe, writer.Build(), ct).ConfigureAwait(false);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new ScheduleException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
            }

            await foreach (var evt in channel.GetEnumerableAsync(ct).ConfigureAwait(false))
            {
                yield return evt;
            }
        }
        finally
        {
            registration.Dispose();
            channel.Dispose();
        }
    }

    private async Task<byte[]> AssertSuccessAsync(ushort messageType, byte[] payload, string operation, CancellationToken ct)
    {
        var response = await _request(messageType, payload, ct).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new ScheduleException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        return reader.IsEof ? [] : reader.ReadBytes(reader.RemainingBytes);
    }
}
