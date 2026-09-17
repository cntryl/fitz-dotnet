namespace Cntryl.Fitz.Testing.Tests;

public sealed class KvScanHelpersTests
{
    [Fact]
    public async Task ShouldRejectImpossibleEmptyContinuationPageGivenScanAll()
    {
        // Arrange
        await using var transaction = new EmptyContinuationTransaction();

        // Act
        var enumerate = async () =>
        {
            await foreach (var _ in transaction.ScanAllAsync())
            {
            }
        };

        // Assert
        await Assert.ThrowsAsync<InvalidOperationException>(enumerate);
    }

    [Fact]
    public async Task ShouldResumeAfterExactLastKeyGivenForwardScanAll()
    {
        // Arrange
        await using var transaction = new RecordingContinuationTransaction();

        // Act
        await foreach (var _ in transaction.ScanAllAsync())
        {
        }

        // Assert
        Assert.Equal(new byte[] { 0x10, 0xFF, 0x00 }, transaction.Queries[1].StartKey!.Value.ToArray());
    }

    sealed class EmptyContinuationTransaction : IKvTransaction
    {
        public Task<KvScanResult> ScanAsync(KvScanQuery query, CancellationToken ct = default) =>
            Task.FromResult(new KvScanResult([], HasMore: true));

        public Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default) => throw new NotSupportedException();
        public Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DeleteRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, CancellationToken ct = default) => throw new NotSupportedException();
        public Task CommitAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task RollbackAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    sealed class RecordingContinuationTransaction : IKvTransaction
    {
        public List<KvScanQuery> Queries { get; } = [];

        public Task<KvScanResult> ScanAsync(KvScanQuery query, CancellationToken ct = default)
        {
            Queries.Add(query);
            return Task.FromResult(Queries.Count == 1
                ? new KvScanResult([new KvPair(new byte[] { 0x10, 0xFF }, Array.Empty<byte>())], HasMore: true)
                : new KvScanResult([], HasMore: false));
        }

        public Task<KvGetResult> GetAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default) => throw new NotSupportedException();
        public Task InsertAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DeleteRangeAsync(ReadOnlyMemory<byte> startKey, ReadOnlyMemory<byte> endKey, CancellationToken ct = default) => throw new NotSupportedException();
        public Task CommitAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public Task RollbackAsync(CancellationToken ct = default) => throw new NotSupportedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
