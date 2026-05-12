using System.IO;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class PublicSurfaceTests
{
    [Fact]
    public void should_not_use_friend_assemblies_in_abstractions()
    {
        var project = ReadRepoFile("src/Abstractions/Abstractions.csproj");
        Assert.DoesNotContain("InternalsVisibleToAttribute", project);
    }

    [Fact]
    public void should_keep_public_abstractions_free_of_hidden_members()
    {
        var subscriptionHandle = ReadRepoFile("src/Abstractions/Runtime/SubscriptionHandle.cs");
        Assert.Contains("public string Pattern", subscriptionHandle);
        Assert.DoesNotContain("internal ulong SubscriptionId", subscriptionHandle);

        var queueItem = ReadRepoFile("src/Abstractions/Domains/Queue/QueueItem.cs");
        Assert.Contains("public string Route", queueItem);
        Assert.Contains("public ReadOnlyMemory<byte> Body", queueItem);
        Assert.Contains("public uint Attempt", queueItem);
        Assert.DoesNotContain("internal ulong Id", queueItem);
        Assert.DoesNotContain("internal ulong Token", queueItem);

        Assert.DoesNotContain("CorrelationId", ReadRepoFile("src/Abstractions/Domains/Rpc/RpcTypes.cs"));
        Assert.DoesNotContain("public ulong Token", ReadRepoFile("src/Abstractions/Domains/Lease/ILease.cs"));
        Assert.DoesNotContain("RequestAsync", ReadRepoFile("src/Abstractions/IClient.cs"));
        Assert.DoesNotContain("SendAsync", ReadRepoFile("src/Core/Client.cs"));
    }

    private static string ReadRepoFile(string relativePath)
    {
        var root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        return File.ReadAllText(Path.Combine(root, relativePath));
    }
}
