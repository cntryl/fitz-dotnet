using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Domains.Lease;

public sealed class LeaseHandle : ILease
{
    private readonly Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;

    internal LeaseHandle(Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> request, string route, ulong token)
    {
        _request = request;
        Route = route;
        Token = token;
    }

    public string Route { get; }

    public ulong Token { get; private set; }

    public Task ExtendAsync(ulong ttlSecs, CancellationToken ct = default)
    {
        return SendTokenTtlAsync(MessageTypes.LeaseRenew, ttlSecs, "EXTEND", ct);
    }

    public async Task ReleaseAsync(CancellationToken ct = default)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteString(string.Empty);
        writer.WriteU64(Token);
        var response = await _request(MessageTypes.LeaseRelease, writer.WrittenMemory, ct).ConfigureAwait(false);
        if (response.Length > 0 && response.Span[0] != 0)
        {
            throw new LeaseException($"RELEASE failed with status {response.Span[0]}", "RELEASE_FAILED", response.Span[0]);
        }

        if (response.Length > 0)
        {
            var reader = new BinaryBufferReader(response);
            _ = reader.ReadU8();
            if (!reader.IsEof)
            {
                throw new LeaseException("RELEASE response has trailing bytes", "RELEASE_INVALID_RESPONSE");
            }
        }
    }

    private async Task SendTokenTtlAsync(ushort messageType, ulong ttlSecs, string operation, CancellationToken ct)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteString(Route);
        writer.WriteString(string.Empty);
        writer.WriteU64(Token);
        writer.WriteU64(ttlSecs);
        var response = await _request(messageType, writer.WrittenMemory, ct).ConfigureAwait(false);
        if (response.Length > 0 && response.Span[0] != 0)
        {
            throw new LeaseException($"{operation} failed with status {response.Span[0]}", $"{operation}_FAILED", response.Span[0]);
        }

        if (response.Length >= 9)
        {
            var reader = new BinaryBufferReader(response);
            _ = reader.ReadU8(); // status
            Token = reader.ReadU64();
            if (!reader.IsEof)
            {
                throw new LeaseException($"{operation} response has trailing bytes", $"{operation}_INVALID_RESPONSE");
            }
        }
    }
}