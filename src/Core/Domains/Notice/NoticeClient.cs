using Cntryl.Fitz.Abstractions.Domains.Notice;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Notice;

public sealed class NoticeClient : INoticeClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task> _send;

    internal NoticeClient(FitzConnection connection)
        : this(connection.SendAsync)
    {
    }

    public NoticeClient(Func<ushort, byte[], CancellationToken, Task> send)
    {
        _send = send;
    }

    public Task PublishAsync(string route, byte[] body, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body);
        return _send(MessageTypes.NoticePublish, writer.Build(), cancellationToken);
    }
}