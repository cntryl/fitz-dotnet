using Cntryl.Fitz.Abstractions.Domains.Queue;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Queue;

public sealed class QueueClient : IQueueClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

    internal QueueClient(FitzConnection connection)
        : this(connection.RequestAsync)
    {
    }

    public QueueClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
    {
        _request = request;
    }

    public async Task<ulong> EnqueueAsync(
        string route,
        byte[] body,
        int? delayMs = null,
        CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body);

        var delaySeconds = (delayMs ?? 0) / 1000;
        writer.WriteU8((byte)(delaySeconds > 0 ? 1 : 0));
        if (delaySeconds > 0)
        {
            writer.WriteU64((ulong)delaySeconds);
        }

        var response = await _request(MessageTypes.QueueEnqueue, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new QueueException($"ENQUEUE failed with status {status}", "ENQUEUE_FAILED", status);
        }

        return reader.IsEof ? 0UL : reader.ReadU64();
    }
}