using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ResponseEnvelopeTests
{
    static readonly WireFailureFactory<KvException> Failure = static failure =>
        new KvException(failure.Message, failure.Code, failure.Status, failure.DomainCode)
        {
            ErrorClass = failure.ErrorClass,
            ErrorEffect = failure.ErrorEffect,
        };

    [Fact]
    public void ShouldReturnReaderAfterStatusGivenSuccessWhenReading()
    {
        // Arrange
        byte[] response = [0, 42];

        // Act
        var reader = ResponseEnvelope.ReadSuccess(response, "OP", Failure);

        // Assert
        Assert.Equal(42, reader.ReadU8());
        Assert.True(reader.IsEof);
    }

    [Fact]
    public void ShouldThrowInvalidResponseGivenEmptyResponseWhenReading()
    {
        // Arrange
        // Act
        var error = Assert.Throws<KvException>(() => ResponseEnvelope.ReadSuccess(ReadOnlyMemory<byte>.Empty, "OP", Failure));

        // Assert
        Assert.Equal("OP_INVALID_RESPONSE", error.Code);
        Assert.Null(error.Status);
    }

    [Fact]
    public void ShouldThrowInvalidResponseGivenUnknownStatusWhenReading()
    {
        // Arrange
        byte[] response = [7];

        // Act
        var error = Assert.Throws<KvException>(() => ResponseEnvelope.ReadSuccess(response, "OP", Failure));

        // Assert
        Assert.Equal("OP_INVALID_RESPONSE", error.Code);
        Assert.Equal((byte)7, error.Status);
    }

    [Fact]
    public void ShouldThrowInvalidResponseGivenMalformedErrorEnvelopeWhenReading()
    {
        // Arrange
        byte[] response = [1, 0];

        // Act
        var error = Assert.Throws<KvException>(() => ResponseEnvelope.ReadSuccess(response, "OP", Failure));

        // Assert
        Assert.Equal("OP_INVALID_RESPONSE", error.Code);
        Assert.Null(error.DomainCode);
    }

    [Fact]
    public void ShouldCarryEnvelopeFieldsGivenErrorResponseWhenReading()
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU16(5003);
        writer.WriteU8(2);
        writer.WriteU8(1);
        writer.WriteString("nope");

        // Act
        var error = Assert.Throws<KvException>(() => ResponseEnvelope.ReadSuccess(writer.Build(), "OP", Failure));

        // Assert
        Assert.Equal("OP_FAILED", error.Code);
        Assert.Equal("OP failed: nope", error.Message);
        Assert.Equal((uint)5003, error.DomainCode);
        Assert.Equal((byte)2, error.ErrorClass);
        Assert.Equal((byte)1, error.ErrorEffect);
    }

    [Fact]
    public void ShouldUseMappedCodeGivenCodeMapperWhenReadingError()
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        writer.WriteU16(9);
        writer.WriteU8(0);
        writer.WriteU8(0);
        writer.WriteString("mapped");

        // Act
        var error = Assert.Throws<KvException>(() => ResponseEnvelope.ReadSuccess(writer.Build(), "OP", Failure, static code => $"CODE_{code}"));

        // Assert
        Assert.Equal("CODE_9", error.Code);
    }
}
