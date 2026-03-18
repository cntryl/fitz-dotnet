using Cntryl.Fitz.Abstractions.Domains.Kv;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Kv;

public sealed class KvTransaction : IKvTransaction
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;
    private readonly string _route;
    private readonly ulong _txId;

    internal KvTransaction(
        Func<ushort, byte[], CancellationToken, Task<byte[]>> request,
        string route,
        ulong txId)
    {
        _request = request;
        _route = route;
        _txId = txId;
    }

    public async Task<KvGetResult> GetAsync(byte[] key, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key);

        var response = await _request(MessageTypes.KvGet, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new KvException($"GET failed with status {status}", "GET_FAILED", status);
        }

        var found = !reader.IsEof && reader.ReadU8() == 1;
        if (!found || reader.IsEof)
        {
            return new KvGetResult(false, null);
        }

        var value = reader.ReadBytes((int)reader.ReadU32());
        return new KvGetResult(true, value);
    }

    public Task PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default)
    {
        return WriteAsync(MessageTypes.KvPut, key, value, "PUT", cancellationToken);
    }

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        return FinalizeAsync(MessageTypes.KvCommit, "COMMIT", cancellationToken);
    }

    public Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        return FinalizeAsync(MessageTypes.KvRollback, "ROLLBACK", cancellationToken);
    }

    private async Task WriteAsync(ushort messageType, byte[] key, byte[] value, string operation, CancellationToken cancellationToken)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        writer.WriteU32((uint)key.Length);
        writer.WriteBytes(key);
        writer.WriteU32((uint)value.Length);
        writer.WriteBytes(value);
        await ExpectStatusAsync(messageType, writer.Build(), operation, cancellationToken);
    }

    private async Task FinalizeAsync(ushort messageType, string operation, CancellationToken cancellationToken)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteU64(_txId);
        writer.WriteString(_route);
        await ExpectStatusAsync(messageType, writer.Build(), operation, cancellationToken);
    }

    private async Task ExpectStatusAsync(ushort messageType, byte[] payload, string operation, CancellationToken cancellationToken)
    {
        var response = await _request(messageType, payload, cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new KvException($"{operation} failed with status {status}", $"{operation}_FAILED", status);
        }
    }
}