using System.Runtime.CompilerServices;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RepositoryPolicyTests
{
    [Fact]
    public void should_keep_one_off_automation_out_of_top_level_scripts_directory()
    {
        var repositoryRoot = GetRepositoryRoot();

        Assert.False(
            Directory.Exists(Path.Combine(repositoryRoot, "scripts")),
            "Top-level scripts directory is not allowed; use .NET tests, projects, or explicit workflow steps.");
    }

    private static string GetRepositoryRoot([CallerFilePath] string sourcePath = "")
    {
        return Path.GetFullPath(Path.Combine(Path.GetDirectoryName(sourcePath)!, "..", "..", ".."));
    }
}
