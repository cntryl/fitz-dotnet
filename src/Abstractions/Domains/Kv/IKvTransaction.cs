namespace Cntryl.Fitz.Abstractions.Domains.Kv;

public interface IKvTransaction
{
    Task<KvGetResult> GetAsync(byte[] key, CancellationToken cancellationToken = default);
    Task PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default);
    Task CommitAsync(CancellationToken cancellationToken = default);
    Task RollbackAsync(CancellationToken cancellationToken = default);
}