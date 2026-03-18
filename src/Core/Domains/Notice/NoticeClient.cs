using System.Runtime.CompilerServices;
using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Notice;

public sealed class NoticeClient : INoticeClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task> _send;
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>>? _request;
    private readonly Action<ushort, Action<byte[]>>? _registerNotificationHandler;
    private readonly Action<ushort>? _unregisterNotificationHandler;

    internal NoticeClient(FitzConnection connection)
        : this(
            connection.SendAsync,
            connection.RequestAsync,
            connection.RegisterNotificationHandler,
            connection.UnregisterNotificationHandler)
    {
    }

    public NoticeClient(
        Func<ushort, byte[], CancellationToken, Task> send,
        Func<ushort, byte[], CancellationToken, Task<byte[]>>? request = null,
        Action<ushort, Action<byte[]>>? registerNotificationHandler = null,
        Action<ushort>? unregisterNotificationHandler = null)
    {
        _send = send;
        _request = request;
        _registerNotificationHandler = registerNotificationHandler;
        _unregisterNotificationHandler = unregisterNotificationHandler;
    }

    public Task PublishAsync(string route, ReadOnlyMemory<byte> body, CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body.Span);
        return _send(MessageTypes.NoticePublish, writer.Build(), ct);
    }

    public async IAsyncEnumerable<NoticeMessage> SubscribeAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_request == null || _registerNotificationHandler == null || _unregisterNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        var channel = new SubscriptionChannel<NoticeMessage>();

        // Register notification handler
        _registerNotificationHandler(MessageTypes.NoticeNotify, payload =>
        {
            try
            {
                var reader = new BinaryBufferReader(payload);
                var eventRoute = reader.ReadString();
                var bodyLength = reader.ReadU32();
                var msgBody = reader.ReadBytes((int)bodyLength);
                channel.PostNotification(new NoticeMessage(eventRoute, msgBody.AsMemory()));
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
            var response = await _request(MessageTypes.NoticeSubscribe, writer.Build(), ct);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new NoticeException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
            }
        }
        catch
        {
            _unregisterNotificationHandler(MessageTypes.NoticeNotify);
            throw;
        }

        // Yield events from the channel
        await foreach (var evt in channel.GetEnumerableAsync(ct))
        {
            yield return evt;
        }
    }
}