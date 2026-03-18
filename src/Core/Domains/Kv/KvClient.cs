using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

public sealed class KvClient : IKvClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

    internal KvClient(FitzConnection connection)
        : this(connection.RequestAsync)
    {
    }

    public KvClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
    {
        _request = request;
    }

    public async Task<IKvTransaction> BeginAsync(
        string route,
        KvMode mode = KvMode.ReadWrite,
        KvDurability durability = KvDurability.Async,
        CancellationToken cancellationToken = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU8((byte)mode);
        writer.WriteU8((byte)durability);

        var response = await _request(MessageTypes.KvBegin, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new KvException($"BEGIN failed with status {status}", "BEGIN_FAILED", status);
        }

        if (reader.IsEof)
        {
            throw new KvException("BEGIN response missing transaction id", "MISSING_TX_ID");
        }

        var txId = reader.ReadU64();
        return new KvTransaction(_request, route, txId);
    }
}