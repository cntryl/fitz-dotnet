namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public interface IStreamClient
{
    Task<IStreamSession> BeginAsync(string route, ReadOnlyMemory<byte>? ingestMetadata = null, CancellationToken ct = default);
    IAsyncEnumerable<StreamRecord> ReadAsync(string route, ulong startOffset, ulong limit = 100, StreamFilterSet? filter = null, ulong? maxBytes = null, ulong? cursorFingerprint = null, ulong? capturedWatermark = null, string? resumeRealm = null, CancellationToken ct = default);
    Task<StreamReadPage> ReadPageAsync(string route, ulong startOffset, ulong limit = 100, StreamFilterSet? filter = null, ulong? maxBytes = null, ulong? cursorFingerprint = null, ulong? capturedWatermark = null, string? resumeRealm = null, CancellationToken ct = default);
    Task<StreamRecord?> PeekAsync(string route, CancellationToken ct = default);
    Task<StreamMetadata> MetadataAsync(string route, CancellationToken ct = default);
    Task<StreamSubscription> SubscribeAsync(
        string pattern,
        CancellationToken ct = default);
}
