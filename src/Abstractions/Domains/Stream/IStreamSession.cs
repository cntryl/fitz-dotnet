namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public interface IStreamSession
{
    Task<ulong?> AppendAsync(byte[] body, byte[]? metadata = null, CancellationToken cancellationToken = default);
    Task CommitAsync(CancellationToken cancellationToken = default);
    Task RollbackAsync(CancellationToken cancellationToken = default);
}