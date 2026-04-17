using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Core;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

public sealed class KvClient : IKvClient
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;

    internal KvClient(FitzConnection connection)
        : this(connection.RequestAsync)
    {
    }

    public KvClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
        : this(async (messageType, payload, ct) => new ReadOnlyMemory<byte>(await request(messageType, payload.ToArray(), ct).ConfigureAwait(false)))
    {
    }

    internal KvClient(Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request)
    {
        _request = request;
    }

    public async Task<IKvTransaction> BeginAsync(
        string route,
        KvMode mode = KvMode.ReadWrite,
        KvDurability durability = KvDurability.Async,
        CancellationToken cancellationToken = default)
    {
        if (!RouteValidation.IsFixedRoute(route, "kv", 3))
        {
            throw new KvException($"route '{route}' must be kv://{{realm}}/{{area}}/{{resource}}", "INVALID_ROUTE");
        }

        using var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteU8((byte)mode);
        writer.WriteU8((byte)durability);

        var response = await _request(MessageTypes.KvBegin, writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new KvException($"BEGIN failed with status {status}", "BEGIN_FAILED", status);
        }

        if (reader.IsEof || reader.RemainingBytes < 8)
        {
            throw new KvException("BEGIN response missing transaction id", "MISSING_TX_ID");
        }

        var txId = reader.ReadU64();
        if (!reader.IsEof)
        {
            throw new KvException("BEGIN response has trailing bytes", "BEGIN_INVALID_RESPONSE");
        }

        return new KvTransaction(_request, route, txId);
    }
}