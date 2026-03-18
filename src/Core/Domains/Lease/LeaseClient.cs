using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseClient : ILeaseClient
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

    internal LeaseClient(FitzConnection connection)
        : this(connection.RequestAsync)
    {
    }

    public LeaseClient(Func<ushort, byte[], CancellationToken, Task<byte[]>> request)
    {
        _request = request;
    }

    public async Task<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        writer.WriteString(string.Empty);
        writer.WriteU64(ttlSecs);
        var response = await _request(MessageTypes.LeaseAcquire, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"ACQUIRE failed with status {status}", "ACQUIRE_FAILED", status);
        }

        if (!reader.IsEof)
        {
            _ = reader.ReadU8();
        }

        if (reader.IsEof)
        {
            throw new LeaseException("ACQUIRE response missing fencing token", "MISSING_TOKEN");
        }

        return new LeaseHandle(_request, route, reader.ReadU64());
    }

    public async Task<LeaseInfo> QueryAsync(string route, CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(route);
        var response = await _request(MessageTypes.LeaseQuery, writer.Build(), cancellationToken);
        var reader = new BinaryBufferReader(response);
        var status = reader.ReadU8();
        if (status != 0)
        {
            throw new LeaseException($"QUERY failed with status {status}", "QUERY_FAILED", status);
        }

        var hasHolder = reader.ReadU8();
        if (hasHolder == 0)
        {
            return new LeaseInfo(false);
        }

        var owner = reader.ReadString();
        var ttlRemaining = reader.ReadU64();
        return new LeaseInfo(true, owner, ttlRemaining);
    }
}