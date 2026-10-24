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
        Assert.Equal(2050u, FitzErrorCodes.StreamConcurrencyConflict);
        Assert.Equal(2051u, FitzErrorCodes.StreamSessionAlreadyActive);
    }

    [Theory]
    [InlineData("APPEND", FitzErrorCodes.StreamConcurrencyConflict, "unrelated wording")]
    [InlineData("COMMIT", FitzErrorCodes.StreamConcurrencyConflict, "unrelated wording")]
    [InlineData("BEGIN", FitzErrorCodes.StreamSessionAlreadyActive, "unrelated wording")]
    [InlineData("COMMIT", 2006u, "backend unavailable")]
    [InlineData("BEGIN", 2052u, "session unavailable")]
    [InlineData("ROLLBACK", 2052u, "session unavailable")]
    [InlineData("LAST", 2006u, "backend unavailable")]
    [InlineData("METADATA", 2006u, "backend unavailable")]
    [InlineData("SUBSCRIBE", 2003u, "invalid pattern")]
    [InlineData("UNSUBSCRIBE", 2006u, "backend unavailable")]
    public void ShouldPreserveVersionedDomainCodeIndependentlyOfWordingGivenErrorEnvelopeWhenDecoding(string operation, uint code, string message)
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        TestErrorEnvelope.Write(writer, code);
        writer.WriteString(message);
        var payload = writer.Build();

        // Act
        var error = Assert.Throws<StreamException>(() => StreamWireHelpers.EnsureSuccessStatusOnly(payload, operation));

        // Assert
        Assert.Equal(code, error.DomainCode);
        Assert.Equal((byte)1, error.Status);
        Assert.Equal($"{operation}_FAILED", error.Code);
        Assert.Contains(message, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("APPEND", null)]
    [InlineData("COMMIT", null)]
    [InlineData("READ", 2001u)]
    public void ShouldRejectLegacyErrorEnvelopesGivenErrorEnvelopeWhenDecoding(string operation, uint? code)
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
        Assert.Null(error.DomainCode);
        Assert.EndsWith("INVALID_RESPONSE", error.Code, StringComparison.Ordinal);
    }
}
