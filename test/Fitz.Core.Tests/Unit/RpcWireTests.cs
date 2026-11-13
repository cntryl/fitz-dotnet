using Cntryl.Fitz.Domains.Rpc;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class RpcWireTests
{
    [Fact]
    public void ShouldRoundTripRequestGivenBudgetWhenEncodingAndDecoding()
    {
        // Arrange
        var correlationId = RpcWire.GuidToNetworkBytes(Guid.NewGuid());
        using var writer = RpcWire.EncodeRequest(correlationId, "rpc://app/echo", [1, 2, 3], TimeSpan.FromMilliseconds(1500));

        // Act
        var request = RpcWire.TryDecodeRequest(writer.Build());

        // Assert
        Assert.NotNull(request);
        Assert.Equal(correlationId, request.CorrelationId);
        Assert.Equal("rpc://app/echo", request.Route);
        Assert.Equal([1, 2, 3], request.Body);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), request.Budget);
        Assert.Equal(RpcWire.GuidFromNetworkBytes(correlationId), request.InvocationId);
    }

    [Fact]
    public void ShouldRejectRequestGivenBudgetAboveOneDayWhenDecoding()
    {
        // Arrange
        using var writer = new BinaryBufferWriter();
        writer.WriteBytes(new byte[RpcWire.CorrelationIdLength]);
        writer.WriteString("rpc://app/echo");
        writer.WriteU32(0);
        writer.WriteU8(1);
        writer.WriteU32(86_400_001);

        // Act
        var request = RpcWire.TryDecodeRequest(writer.Build());

        // Assert
        Assert.Null(request);
    }

    [Fact]
    public void ShouldRoundTripTerminalErrorGivenEncodedErrorWhenDecoding()
    {
        // Arrange
        var body = RpcWire.EncodeTerminalError(FitzErrorCodes.RpcBackpressure, "busy", 5, 0);

        // Act
        var error = RpcWire.TryDecodeTerminalError(body);

        // Assert
        Assert.NotNull(error);
        Assert.Equal("BUSY", error.Code);
        Assert.Equal("busy", error.Message);
        Assert.Equal((byte)5, error.ErrorClass);
        Assert.Equal((byte)0, error.ErrorEffect);
    }

    [Fact]
    public void ShouldTreatBodyAsDataGivenNonErrorPayloadWhenDecodingTerminalError()
    {
        // Arrange
        byte[] body = [0, 1, 2];

        // Act
        var error = RpcWire.TryDecodeTerminalError(body);

        // Assert
        Assert.Null(error);
    }
}
