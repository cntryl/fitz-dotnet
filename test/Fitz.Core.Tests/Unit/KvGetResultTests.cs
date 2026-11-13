namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class KvGetResultTests
{
    [Fact]
    public void ShouldSelectFoundCaseGivenValueWhenSwitchingOverResult()
    {
        // Arrange
        KvGetResult result = new KvFound("alice"u8.ToArray());

        // Act
        var described = Describe(result);

        // Assert
        Assert.Equal("found alice", described);
    }

    [Fact]
    public void ShouldSelectNotFoundCaseGivenMissingKeyWhenSwitchingOverResult()
    {
        // Arrange
        KvGetResult result = KvNotFound.Instance;

        // Act
        var described = Describe(result);

        // Assert
        Assert.Equal("not found", described);
    }

    [Fact]
    public void ShouldExposeEmptyValueAsFoundGivenKeyHoldingNoBytesWhenMatching()
    {
        // Arrange
        KvGetResult result = new KvFound(ReadOnlyMemory<byte>.Empty);

        // Act
        var found = Assert.IsType<KvFound>(result.Value);

        // Assert
        Assert.True(found.Value.IsEmpty);
    }

    [Fact]
    public void ShouldCompareByCaseAndContentsGivenTwoResultsWhenTestingEquality()
    {
        // Arrange
        var bytes = "alice"u8.ToArray();
        KvGetResult found = new KvFound(bytes);
        KvGetResult sameFound = new KvFound(bytes);
        KvGetResult otherFound = new KvFound("bob"u8.ToArray());
        KvGetResult missing = KvNotFound.Instance;
        KvGetResult alsoMissing = KvNotFound.Instance;

        // Act
        var equalFound = found == sameFound;
        var differentContents = found != otherFound;
        var differentCases = found != missing;
        var equalMissing = missing.Equals(alsoMissing);

        // Assert
        Assert.True(equalFound);
        Assert.True(differentContents);
        Assert.True(differentCases);
        Assert.True(equalMissing);
        Assert.Equal(found.GetHashCode(), sameFound.GetHashCode());
    }

    // No default arm: the compiler checks that both cases are handled.
    static string Describe(KvGetResult result) => result switch
    {
        KvFound found => "found " + System.Text.Encoding.UTF8.GetString(found.Value.Span),
        KvNotFound => "not found",
    };
}
