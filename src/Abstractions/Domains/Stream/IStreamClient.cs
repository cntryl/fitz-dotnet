namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public interface IStreamClient
{
    Task<IStreamSession> BeginAsync(string route, ulong expectedOffset = 0, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default);
    IAsyncEnumerable<StreamRecord> ReadAsync(string route, ulong startOffset, ulong limit = 100, ulong? maxBytes = null, CancellationToken ct = default);
    Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default);
}