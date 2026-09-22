using Cntryl.Fitz.Domains.Stream;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class StreamErrorEnvelopeTests
{
    [Fact]
    public void ShouldMatchWireProtocolGivenStreamWriteContentionCodesWhenValidated()
    {
        // Arrange
        // Act
        // Assert
        Assert.Equal(2001u, FitzErrorCodes.StreamConcurrencyConflict);
        Assert.Equal(2002u, FitzErrorCodes.StreamSessionAlreadyActive);
    }

    [Theory]
    [InlineData("APPEND", FitzErrorCodes.StreamConcurrencyConflict, "unrelated wording")]
    [InlineData("COMMIT", FitzErrorCodes.StreamConcurrencyConflict, "unrelated wording")]
    [InlineData("BEGIN", FitzErrorCodes.StreamSessionAlreadyActive, "unrelated wording")]
    [InlineData("COMMIT", 2012u, "backend unavailable")]
    [InlineData("BEGIN", 2003u, "session unavailable")]
    [InlineData("ROLLBACK", 2003u, "session unavailable")]
    [InlineData("LAST", 2012u, "backend unavailable")]
    [InlineData("METADATA", 2012u, "backend unavailable")]
    [InlineData("SUBSCRIBE", 2010u, "invalid pattern")]
    [InlineData("UNSUBSCRIBE", 2012u, "backend unavailable")]
    public void ShouldPreserveVersionedDomainCodeIndependentlyOfWordingGivenErrorEnvelopeWhenDecoding(string operation, uint code, string message)
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(2);
        writer.WriteU32(code);
        writer.WriteString(message);
        var payload = writer.Build();

        // Act
        var error = Assert.Throws<StreamException>(() => StreamWireHelpers.EnsureSuccessStatusOnly(payload, operation));

        // Assert
        Assert.Equal(code, error.DomainCode);
        Assert.Equal((byte)2, error.Status);
        Assert.Equal($"{operation}_FAILED", error.Code);
        Assert.Contains(message, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("APPEND", null)]
    [InlineData("COMMIT", null)]
    [InlineData("READ", 2001u)]
    public void ShouldPreserveLegacyErrorEnvelopesGivenErrorEnvelopeWhenDecoding(string operation, uint? code)
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        if (code.HasValue)
            writer.WriteU32(code.Value);
        writer.WriteString("concurrency conflict");
        var payload = writer.Build();

        // Act
        var error = Assert.Throws<StreamException>(() => StreamWireHelpers.EnsureSuccessStatusOnly(payload, operation));

        // Assert
        Assert.Equal(code, error.DomainCode);
        Assert.Equal((byte)1, error.Status);
    }
}
