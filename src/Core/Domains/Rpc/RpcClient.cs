using System.Security.Cryptography;
using Cntryl.Fitz.Abstractions.Domains.Rpc;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Rpc;

public sealed class RpcClient : IRpcClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

    internal RpcClient(FitzConnection connection)
        : this(connection.RequestAsync)
    {
    }

    public RpcClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
    {
        _request = request;
    }

    public async Task RequestAsync(string route, byte[] body, CancellationToken cancellationToken = default)
    {
        var correlationId = new byte[16];
        RandomNumberGenerator.Fill(correlationId);

        var writer = new BinaryBufferWriter();
        writer.WriteU32((uint)correlationId.Length);
        writer.WriteBytes(correlationId);
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU32((uint)body.Length);
        writer.WriteBytes(body);

        var response = await _request(MessageTypes.RpcRequest, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new RpcException($"REQUEST failed with status {status}", "REQUEST_FAILED", status);
        }
    }
}