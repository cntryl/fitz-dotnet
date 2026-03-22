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
    private readonly Func<ushort, Action<byte[]>, IDisposable>? _registerNotificationHandler;

    internal NoticeClient(FitzConnection connection)
        : this(
            connection.SendAsync,
            connection.RequestAsync,
            connection.RegisterNotificationHandler)
    {
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

    public async IAsyncEnumerable<NoticeMessage> SubscribeAsync(
        string pattern,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_request == null || _registerNotificationHandler == null)
        {
            throw new InvalidOperationException("Notification handlers not configured for subscription support");
        }

        var channel = new SubscriptionChannel<NoticeMessage>();
        var registration = _registerNotificationHandler(MessageTypes.NoticeNotify, payload =>
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

        using var writer = new BinaryBufferWriter();
        writer.WriteString(pattern);

        try
        {
            var response = await _request(MessageTypes.NoticeSubscribe, writer.Build(), ct).ConfigureAwait(false);
            var reader = new BinaryBufferReader(response);
            var status = reader.ReadU8();
            if (status != 0)
            {
                throw new NoticeException($"SUBSCRIBE failed with status {status}", "SUBSCRIBE_FAILED", status);
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
}
