namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public interface IStreamClient
{
    Task<IStreamSession> BeginAsync(string route, ulong expectedOffset = 0, byte[]? ingestMetadata = null, CancellationToken cancellationToken = default);
    IAsyncEnumerable<StreamRecord> ReadAsync(string route, ulong startOffset, ulong limit = 100, ulong? maxBytes = null, CancellationToken cancellationToken = default);
    Task<StreamMetadata> MetadataAsync(string route, CancellationToken cancellationToken = default);
}