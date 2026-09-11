namespace Cntryl.Fitz.Abstractions.Domains.Lease;

public interface ILease : IAsyncDisposable
{
    string Route { get; }
    /// <summary>The immutable fencing token issued for this lease generation.</summary>
    ulong FencingToken { get; }
    Task ExtendAsync(ulong ttlSecs, CancellationToken ct = default);
    Task ReleaseAsync(CancellationToken ct = default);
}
