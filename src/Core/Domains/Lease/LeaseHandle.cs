using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseHandle : ILease
{
    private readonly Func<ushort, byte[], CancellationToken, Task<byte[]>> _request;

    internal LeaseHandle(Func<ushort, byte[], CancellationToken, Task<byte[]>> request, string route, ulong token)
    {
        _request = request;
        Route = route;
        Token = token;
    }

    public string Route { get; }

    public ulong Token { get; }

    public Task ExtendAsync(ulong ttlSecs, CancellationToken cancellationToken = default)
    {
        return SendTokenTtlAsync(MessageTypes.LeaseRenew, ttlSecs, "EXTEND", cancellationToken);
    }

    public Task RenewAsync(ulong ttlSecs, CancellationToken cancellationToken = default)
    {
        return SendTokenTtlAsync(MessageTypes.LeaseRenew, ttlSecs, "RENEW", cancellationToken);
    }

    public async Task ReleaseAsync(CancellationToken cancellationToken = default)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteString(string.Empty);
        writer.WriteU64(Token);
        var response = await _request(MessageTypes.LeaseRelease, writer.Build(), cancellationToken);
        if (response.Length > 0 && response[0] != 0)
        {
            throw new LeaseException($"RELEASE failed with status {response[0]}", "RELEASE_FAILED", response[0]);
        }
    }

    private async Task SendTokenTtlAsync(ushort messageType, ulong ttlSecs, string operation, CancellationToken cancellationToken)
    {
        var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteString(string.Empty);
        writer.WriteU64(Token);
        writer.WriteU64(ttlSecs);
        var response = await _request(messageType, writer.Build(), cancellationToken);
        if (response.Length > 0 && response[0] != 0)
        {
            throw new LeaseException($"{operation} failed with status {response[0]}", $"{operation}_FAILED", response[0]);
        }
    }
}