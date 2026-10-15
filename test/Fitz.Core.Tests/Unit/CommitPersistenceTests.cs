using Cntryl.Fitz.Domains.Kv;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class CommitPersistenceTests
{
    [Theory]
    [InlineData(KvDurability.Buffered, 0)]
    [InlineData(KvDurability.Sync, 1)]
    public async Task ShouldEncodePersistenceAtCommitGivenExplicitChoiceWhenFinalizing(KvDurability durability, byte wire)
    {
        // Arrange
        const string route = "kv://r/a/x";
        byte[] beginPayload = [];
        byte[] commitPayload = [];
        using var client = new KvClient((kind, payload, _) =>
        {
            using var writer = new BinaryBufferWriter();
            writer.WriteU8(0);
            if (kind == MessageTypes.KvBegin)
            {
                beginPayload = payload.ToArray();
                writer.WriteU64(7);
            }
            if (kind == MessageTypes.KvCommit)
            {
                commitPayload = payload.ToArray();
            }
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(writer.Build());
        });

        // Act
        await using var transaction = await client.BeginAsync(route);
        await transaction.CommitAsync(durability);

        // Assert
        Assert.Equal(4 + route.Length + 1, beginPayload.Length);
        Assert.Equal(8 + 4 + route.Length + 1, commitPayload.Length);
        Assert.Equal(wire, commitPayload[^1]);
    }
}
