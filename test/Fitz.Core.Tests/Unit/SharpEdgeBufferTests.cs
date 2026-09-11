using Cntryl.Fitz.Domains.Notice;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SharpEdgeBufferTests
{
    [Fact]
    public void ShouldThrowBeforeAllocatingGivenDisposedWriterWhenBuilding()
    {
        // Arrange
        var writer = new BinaryBufferWriter();
        writer.WriteBytes(new byte[ushort.MaxValue]);
        writer.Dispose();


        // Act
        var before = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        Assert.Throws<ObjectDisposedException>(writer.Build);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 8192, $"Disposed Build allocated {allocated} bytes.");
    }

    [Fact]
    public async Task ShouldKeepPooledPayloadAliveGivenNoticePublishWhenSendCompletes()
    {
        // Arrange
        var sendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[]? observed = null;

        Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask> send = async (_, payload, cancellationToken) =>
        {
            sendStarted.TrySetResult();
            await allowRead.Task.WaitAsync(cancellationToken);
            observed = payload.ToArray();
        };
        using var client = new NoticeClient(send);

        using var expectedWriter = new BinaryBufferWriter();
        expectedWriter.WriteString("notice://prod/app/events");
        expectedWriter.WriteU32(4);
        expectedWriter.WriteBytes("body"u8);
        var expected = expectedWriter.Build();

        var publish = client.PublishAsync("notice://prod/app/events", "body"u8.ToArray());
        await sendStarted.Task;
        allowRead.TrySetResult();

        // Act
        await publish;


        // Assert
        Assert.Equal(expected, observed);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ShouldKeepPooledPayloadAliveGivenStreamFinalizationWhenRequestCompletes(bool commit)
    {
        // Arrange
        var requestStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[]? observed = null;

        await using var session = new StreamSession(async (_, payload, cancellationToken) =>
        {
            requestStarted.TrySetResult();
            await allowRead.Task.WaitAsync(cancellationToken);
            observed = payload.ToArray();
            return commit
                ? new ReadOnlyMemory<byte>([0, 0, 0, 0, 0])
                : new ReadOnlyMemory<byte>([0]);
        }, 42);

        using var expectedWriter = new BinaryBufferWriter();
        expectedWriter.WriteU64(42);
        if (commit)
        {
            expectedWriter.WriteU8(0);
        }

        var finalize = commit ? session.CommitAsync() : session.RollbackAsync();
        await requestStarted.Task;
        allowRead.TrySetResult();

        // Act
        await finalize;


        // Assert
        Assert.Equal(expectedWriter.Build(), observed);
    }

    [Fact]
    public void ShouldRejectUntrustedLengthGivenBinaryReaderWhenAllocating()
    {
        // Arrange
        // Act
        // Assert
        var reader = new BinaryBufferReader(new byte[] { 0x7F, 0xFF, 0xFF, 0xFF });

        var exception = Assert.Throws<ProtocolException>(() => reader.ReadBytes(reader.ReadU32()));

        Assert.Contains("exceeds", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ShouldRejectBufferAccessGivenDisposedWriterWhenReadingOutput()
    {
        // Arrange
        var writer = new BinaryBufferWriter();
        writer.WriteU8(1);

        // Act
        writer.Dispose();


        // Assert
        Assert.Throws<ObjectDisposedException>(() => _ = writer.WrittenMemory);
        Assert.Throws<ObjectDisposedException>(() => _ = writer.Build());
    }

    [Fact]
    public void ShouldRejectBufferAccessGivenDisposedPooledFrameWhenReadingPayload()
    {
        // Arrange
        var frame = PooledFrame.FromRentedBuffer(System.Buffers.ArrayPool<byte>.Shared.Rent(8), 1);

        // Act
        frame.Dispose();


        // Assert
        Assert.Throws<ObjectDisposedException>(() => _ = frame.Memory);
    }
}
