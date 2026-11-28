using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ScopedDropTests
{
    [Theory]
    [InlineData("kv://prod/*", KvDurability.Sync)]
    [InlineData("prod/app/*", KvDurability.Buffered)]
    [InlineData("kv://prod/app/data", KvDurability.Sync)]
    public async Task ShouldEncodeKvDropGivenAValidScope(string selector, KvDurability persistence)
    {
        // Arrange
        var calls = 0;
        await using var client = new KvClient((type, payload, _) =>
        {
            calls++;
            Assert.Equal((ushort)112, type);
            AssertPayload(payload, selector, (byte)persistence);
            return Task.FromResult<byte[]>([0]);
        });

        // Act
        await client.DropAsync(selector, persistence);

        // Assert
        Assert.Equal(1, calls);
    }

    [Theory]
    [InlineData("stream://prod/*", StreamCommitMode.Sync)]
    [InlineData("prod/app/*", StreamCommitMode.Buffered)]
    [InlineData("stream://prod/app/events", StreamCommitMode.Sync)]
    public async Task ShouldEncodeStreamDropGivenAValidScope(string selector, StreamCommitMode persistence)
    {
        // Arrange
        var calls = 0;
        await using var client = new StreamClient((type, payload, _) =>
        {
            calls++;
            Assert.Equal((ushort)610, type);
            AssertPayload(payload, selector, (byte)persistence);
            return Task.FromResult<byte[]>([0]);
        });

        // Act
        await client.DropAsync(selector, persistence);

        // Assert
        Assert.Equal(1, calls);
    }

    [Theory]
    [InlineData("kv://prod/*/*")]
    [InlineData("kv://*/app/data")]
    [InlineData("kv://prod/app/**")]
    [InlineData("stream://prod/app/data")]
    public async Task ShouldRejectKvDropGivenAnInvalidScope(string selector)
    {
        // Arrange
        await using var client = new KvClient((_, _, _) => throw new InvalidOperationException("Unexpected dispatch"));

        // Act
        var failure = await Record.ExceptionAsync(() => client.DropAsync(selector, KvDurability.Sync));

        // Assert
        Assert.IsType<ArgumentException>(failure);
    }

    [Theory]
    [InlineData("stream://prod/*/*")]
    [InlineData("stream://prod/__realm__/events")]
    [InlineData("stream://prod/app/__area__")]
    public async Task ShouldRejectStreamDropGivenAnInvalidScope(string selector)
    {
        // Arrange
        await using var client = new StreamClient((_, _, _) => throw new InvalidOperationException("Unexpected dispatch"));

        // Act
        var failure = await Record.ExceptionAsync(() => client.DropAsync(selector, StreamCommitMode.Sync));

        // Assert
        Assert.IsType<ArgumentException>(failure);
    }

    [Fact]
    public async Task ShouldRejectKvDropGivenAnUnknownPersistencePolicy()
    {
        // Arrange
        await using var client = new KvClient((_, _, _) => throw new InvalidOperationException("Unexpected dispatch"));

        // Act
        var failure = await Record.ExceptionAsync(() => client.DropAsync("kv://prod/app/data", (KvDurability)2));

        // Assert
        Assert.IsType<ArgumentOutOfRangeException>(failure);
    }

    [Fact]
    public async Task ShouldRejectStreamDropGivenTrailingSuccessBytes()
    {
        // Arrange
        await using var client = new StreamClient((_, _, _) => Task.FromResult<byte[]>([0, 1]));

        // Act
        var failure = await Assert.ThrowsAsync<StreamException>(
            () => client.DropAsync("stream://prod/app/events", StreamCommitMode.Sync));

        // Assert
        Assert.Equal("DROP_INVALID_RESPONSE", failure.Code);
    }

    [Fact]
    public async Task ShouldPreserveDropFailureGivenAnIndeterminateBrokerOutcome()
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU16(1053);
        writer.WriteU8(3);
        writer.WriteU8(2);
        writer.WriteString("outcome unknown");
        var calls = 0;
        await using var client = new KvClient((_, _, _) =>
        {
            calls++;
            return Task.FromResult(writer.WrittenMemory.ToArray());
        });

        // Act
        var failure = await Assert.ThrowsAsync<KvException>(
            () => client.DropAsync("kv://prod/app/data", KvDurability.Sync));

        // Assert
        Assert.Equal(1, calls);
        Assert.Equal(1053u, failure.DomainCode);
        Assert.Equal((byte)2, failure.ErrorEffect);
    }

    static void AssertPayload(byte[] payload, string selector, byte persistence)
    {
        var reader = new BinaryBufferReader(payload);
        Assert.Equal(selector, reader.ReadString());
        Assert.Equal(persistence, reader.ReadU8());
        Assert.True(reader.IsEof);
    }
}
