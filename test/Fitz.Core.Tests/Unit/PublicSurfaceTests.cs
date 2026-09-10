using System.IO;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class PublicSurfaceTests
{
    [Fact]
    public async Task ShouldAllowRetryGivenUnsubscribeFailure()
    {
        var expected = new InvalidOperationException("unsubscribe failed");
        var attempts = 0;
        await using var handle = new TestSubscriptionHandle(_ =>
        {
            attempts++;
            return attempts == 1 ? ValueTask.FromException(expected) : ValueTask.CompletedTask;
        });

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => handle.UnsubscribeAsync().AsTask());
        await handle.UnsubscribeAsync();
        await handle.Completion;

        Assert.Same(expected, thrown);
        Assert.Equal(2, attempts);
    }

    [Fact]
    public void ShouldNotUseFriendAssembliesInAbstractions()
    {
        var project = ReadRepoFile("src/Fitz.Abstractions/Fitz.Abstractions.csproj");
        Assert.DoesNotContain("InternalsVisibleToAttribute", project, StringComparison.Ordinal);
    }

    [Fact]
    public void ShouldKeepPublicAbstractionsFreeOfHiddenMembers()
    {
        var subscriptionHandle = ReadRepoFile("src/Fitz.Abstractions/Runtime/SubscriptionHandle.cs");
        Assert.Contains("public string Pattern", subscriptionHandle, StringComparison.Ordinal);
        Assert.Contains("public Task Completion", subscriptionHandle, StringComparison.Ordinal);
        Assert.DoesNotContain("internal ulong SubscriptionId", subscriptionHandle, StringComparison.Ordinal);

        var queueItem = ReadRepoFile("src/Fitz.Abstractions/Domains/Queue/QueueItem.cs");
        Assert.Contains("public string Route", queueItem, StringComparison.Ordinal);
        Assert.Contains("public ReadOnlyMemory<byte> Body", queueItem, StringComparison.Ordinal);
        Assert.Contains("public uint Attempt", queueItem, StringComparison.Ordinal);
        Assert.DoesNotContain("internal ulong Id", queueItem, StringComparison.Ordinal);
        Assert.DoesNotContain("internal ulong Token", queueItem, StringComparison.Ordinal);

        Assert.DoesNotContain("CorrelationId", ReadRepoFile("src/Fitz.Abstractions/Domains/Rpc/RpcTypes.cs"), StringComparison.Ordinal);
        Assert.Contains("ulong FencingToken", ReadRepoFile("src/Fitz.Abstractions/Domains/Lease/ILease.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("public ulong Token", ReadRepoFile("src/Fitz.Abstractions/Domains/Lease/ILease.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("RequestAsync", ReadRepoFile("src/Fitz.Abstractions/IClient.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("SendAsync", ReadRepoFile("src/Fitz.Core/Client.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void ShouldExposeDomainClientsAsPropertiesAndTransactionalHandlesAsAsyncDisposable()
    {
        // Arrange
        var client = ReadRepoFile("src/Fitz.Abstractions/IClient.cs");

        // Act
        var transaction = ReadRepoFile("src/Fitz.Abstractions/Domains/Kv/IKvTransaction.cs");
        var streamSession = ReadRepoFile("src/Fitz.Abstractions/Domains/Stream/IStreamSession.cs");

        // Assert
        Assert.Contains("IKvClient Kv { get; }", client, StringComparison.Ordinal);
        Assert.DoesNotContain("IKvClient Kv()", client, StringComparison.Ordinal);
        Assert.Contains("IKvTransaction : IAsyncDisposable", transaction, StringComparison.Ordinal);
        Assert.Contains("IStreamSession : IAsyncDisposable", streamSession, StringComparison.Ordinal);
    }

    static string ReadRepoFile(string relativePath)
    {
        var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        return File.ReadAllText(Path.Combine(root, relativePath));
    }

    sealed class TestSubscriptionHandle(Func<CancellationToken, ValueTask> unsubscribe)
        : SubscriptionHandle("notice://realm/area/resource", unsubscribe)
    {
    }
}
