using Cntryl.Fitz.Domains.Queue;
using Cntryl.Fitz.Domains.Schedule;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class QueueBackpressureTests
{
    [Fact]
    public async Task ShouldPreserveCodedCapacityRejectionGivenReservedItemWhenCompleting()
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU32(4005);
        writer.WriteString("not accepted");
        await using var item = new QueueReservedItem("queue://prod/app/tasks", ReadOnlyMemory<byte>.Empty,
            QueueItem.AttemptUnavailable, 7, 11, (_, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build()));
        // Act
        var error = await Assert.ThrowsAsync<QueueException>(() => item.CompleteAsync());
        // Assert
        Assert.Equal(4005u, error.DomainCode);
        Assert.True(Retryability.IsRetryable(error));
    }

    [Fact]
    public async Task ShouldPreserveCodedCapacityGivenScheduleWhenCanceling()
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU32(7010);
        writer.WriteString("not accepted");
        using var schedule = new ScheduleClient((_, _, _) => Task.FromResult(writer.Build()));
        // Act
        var error = await Assert.ThrowsAsync<ScheduleException>(() => schedule.CancelAsync("schedule://prod/app/jobs/run"));
        // Assert
        Assert.Equal(7010u, error.DomainCode);
        Assert.True(Retryability.IsRetryable(error));
    }

    [Theory]
    [InlineData(4005u, true)]
    [InlineData(4007u, false)]
    public async Task ShouldSurfaceCodedRejectionGivenReservedItemWhenExtending(uint code, bool retryable)
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU32(code);
        writer.WriteString("broker rejection");
        var calls = 0;
        await using var item = new QueueReservedItem("queue://prod/app/tasks", ReadOnlyMemory<byte>.Empty,
            QueueItem.AttemptUnavailable, 7, 11, (_, _, _) =>
            {
                calls++;
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
            });
        // Act
        var error = await Assert.ThrowsAsync<QueueException>(() => item.ExtendAsync(TimeSpan.FromSeconds(30)));
        // Assert
        Assert.Equal(code, error.DomainCode);
        Assert.Equal(retryable, Retryability.IsRetryable(error));
        Assert.Equal(1, calls);
    }

    [Fact]
    public void ShouldPreservePlainMessageGivenLongErrorWhenReadingQueueResponse()
    {
        // Arrange
        var message = new string('x', 4005);
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteString(message);
        // Act
        var error = QueueWireHelpers.ReadError(writer.Build(), "COMPLETE", 1);
        // Assert
        Assert.Null(error.DomainCode);
        Assert.Contains(message, error.Message, StringComparison.Ordinal);
        Assert.False(Retryability.IsRetryable(error));
    }

    [Fact]
    public void ShouldRejectMalformedCapacityGivenTruncatedErrorWhenReadingQueueResponse()
    {
        // Arrange
        byte[] payload = [1, 0, 0, 15, 165, 0, 0, 0, 2, 120];
        // Act
        var error = QueueWireHelpers.ReadError(payload, "COMPLETE", 1);
        // Assert
        Assert.Null(error.DomainCode);
        Assert.False(Retryability.IsRetryable(error));
    }

    [Theory]
    [InlineData(new byte[] { 2, 0, 0, 15, 165, 0, 0, 0, 0 })]
    [InlineData(new byte[] { 1, 0, 0, 15, 165 })]
    public async Task ShouldRejectInvalidCapacityEnvelopeGivenQueueWhenReserving(byte[] response)
    {
        // Arrange
        using var queue = new QueueClient((_, _, _) => Task.FromResult(response));
        // Act
        var error = await Assert.ThrowsAsync<QueueException>(() => queue.ReserveAsync("queue://prod/app/jobs", TimeSpan.FromSeconds(30)));
        // Assert
        Assert.Null(error.DomainCode);
        Assert.False(Retryability.IsRetryable(error));
    }
}
