namespace Cntryl.Fitz.Abstractions.Domains.Kv;

public interface IKvTransaction
{
    Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default);
    Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default);
    Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default);
    Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default);
    Task DeleteRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, CancellationToken ct = default);
    IAsyncEnumerable<KvPair> ScanAsync(KvScanQuery query, CancellationToken ct = default);
    Task CommitAsync(CancellationToken ct = default);
    Task RollbackAsync(CancellationToken ct = default);
}