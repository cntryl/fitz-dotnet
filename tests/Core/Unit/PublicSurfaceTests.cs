using System.IO;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class PublicSurfaceTests
{
    [Fact]
    public void should_not_use_friend_assemblies_in_abstractions()
    {
        var project = ReadRepoFile("src/Abstractions/Abstractions.csproj");
        Assert.DoesNotContain("InternalsVisibleToAttribute", project, StringComparison.Ordinal);
    }

    [Fact]
    public void should_keep_public_abstractions_free_of_hidden_members()
    {
        var subscriptionHandle = ReadRepoFile("src/Abstractions/Runtime/SubscriptionHandle.cs");
        Assert.Contains("public string Pattern", subscriptionHandle, StringComparison.Ordinal);
        Assert.DoesNotContain("internal ulong SubscriptionId", subscriptionHandle, StringComparison.Ordinal);

        var queueItem = ReadRepoFile("src/Abstractions/Domains/Queue/QueueItem.cs");
        Assert.Contains("public string Route", queueItem, StringComparison.Ordinal);
        Assert.Contains("public ReadOnlyMemory<byte> Body", queueItem, StringComparison.Ordinal);
        Assert.Contains("public uint Attempt", queueItem, StringComparison.Ordinal);
        Assert.DoesNotContain("internal ulong Id", queueItem, StringComparison.Ordinal);
        Assert.DoesNotContain("internal ulong Token", queueItem, StringComparison.Ordinal);

        Assert.DoesNotContain("CorrelationId", ReadRepoFile("src/Abstractions/Domains/Rpc/RpcTypes.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("public ulong Token", ReadRepoFile("src/Abstractions/Domains/Lease/ILease.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("RequestAsync", ReadRepoFile("src/Abstractions/IClient.cs"), StringComparison.Ordinal);
        Assert.DoesNotContain("SendAsync", ReadRepoFile("src/Core/Client.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void should_expose_domain_clients_as_properties_and_transactional_handles_as_async_disposable()
    {
        // Arrange
        var client = ReadRepoFile("src/Abstractions/IClient.cs");

        // Act
        var transaction = ReadRepoFile("src/Abstractions/Domains/Kv/IKvTransaction.cs");
        var streamSession = ReadRepoFile("src/Abstractions/Domains/Stream/IStreamSession.cs");

        // Assert
        Assert.Contains("IKvClient Kv { get; }", client, StringComparison.Ordinal);
        Assert.DoesNotContain("IKvClient Kv()", client, StringComparison.Ordinal);
        Assert.Contains("IKvTransaction : IAsyncDisposable", transaction, StringComparison.Ordinal);
        Assert.Contains("IStreamSession : IAsyncDisposable", streamSession, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relativePath)
    {
        var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        return File.ReadAllText(Path.Combine(root, relativePath));
    }
}
