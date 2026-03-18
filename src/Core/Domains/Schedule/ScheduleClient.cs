using Cntryl.Fitz.Abstractions.Domains.Schedule;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Schedule;

public sealed class ScheduleClient : IScheduleClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

    internal ScheduleClient(FitzConnection connection)
        : this(connection.RequestAsync)
    {
    }

    public ScheduleClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
    {
        _request = request;
    }

    public async Task<string?> CreateAsync(string route, string cron, byte[] payload, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(cron);
        writer.WriteU32((uint)payload.Length);
        writer.WriteBytes(payload);
        var data = await AssertSuccessAsync(MessageTypes.ScheduleCreate, writer.Build(), "CREATE", cancellationToken);
        var reader = new BinaryBufferReader(data);
        return !reader.IsEof && reader.ReadU8() == 1 ? reader.ReadString() : null;
    }

    public async Task CancelAsync(string route, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        _ = await AssertSuccessAsync(MessageTypes.ScheduleCancel, writer.Build(), "CANCEL", cancellationToken);
    }

    private async Task<byte[]> AssertSuccessAsync(ushort messageType, byte[] payload, string operation, CancellationToken cancellationToken)
    {
        var response = await _request(messageType, payload, cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new ScheduleException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }

        return reader.IsEof ? [] : reader.ReadBytes(reader.RemainingBytes);
    }
}