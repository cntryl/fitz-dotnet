using System.Runtime.CompilerServices;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RepositoryPolicyTests
{
    [Fact]
    public void ShouldKeepOneOffAutomationOutOfTopLevelScriptsDirectory()
    {
        var repositoryRoot = GetRepositoryRoot();

        Assert.False(
            Directory.Exists(Path.Combine(repositoryRoot, "scripts")),
            "Top-level scripts directory is not allowed; use .NET tests, projects, or explicit workflow steps.");
    }

    static string GetRepositoryRoot([CallerFilePath] string sourcePath = "") => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(sourcePath)!, "..", "..", ".."));
}
