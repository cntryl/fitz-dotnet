namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class WireDurationTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(30)]
    [InlineData(86_400)]
    public void ShouldConvertWholeSecondsGivenDurationWhenEncodedForWire(int seconds)
    {
        // Arrange
        var value = TimeSpan.FromSeconds(seconds);

        // Act
        var encoded = WireDuration.ToSeconds(value, "ttl");

        // Assert
        Assert.Equal((ulong)seconds, encoded);
        Assert.True(WireDuration.TryFromSeconds(encoded, out var roundTripped));
        Assert.Equal(value, roundTripped);
    }

    // The wire carries whole seconds, and rounding would hand back a shorter hold than the
    // caller asked for. That difference only shows up under contention, so it is rejected.
    [Theory]
    [InlineData(1)]
    [InlineData(500)]
    [InlineData(1_500)]
    [InlineData(30_001)]
    public void ShouldRejectSubSecondPrecisionGivenDurationWhenEncodedForWire(int milliseconds)
    {
        // Arrange
        var value = TimeSpan.FromMilliseconds(milliseconds);

        // Act
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => WireDuration.ToSeconds(value, "ttl"));

        // Assert
        Assert.Equal("ttl", error.ParamName);
    }

    [Fact]
    public void ShouldRejectNegativeDurationGivenDurationWhenEncodedForWire()
    {
        // Arrange
        var value = TimeSpan.FromSeconds(-1);

        // Act
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => WireDuration.ToSeconds(value, "wait"));

        // Assert
        Assert.Equal("wait", error.ParamName);
    }

    [Fact]
    public void ShouldDecodeWholeSecondsGivenWireValueWhenWithinTimeSpanRange()
    {
        // Arrange
        const ulong seconds = 90;

        // Act
        var decoded = WireDuration.TryFromSeconds(seconds, out var value);

        // Assert
        Assert.True(decoded);
        Assert.Equal(TimeSpan.FromSeconds(90), value);
    }

    [Fact]
    public void ShouldReportFailureGivenWireValueWhenBeyondTimeSpanRange()
    {
        // Arrange
        const ulong seconds = ulong.MaxValue;

        // Act
        var decoded = WireDuration.TryFromSeconds(seconds, out var value);

        // Assert
        Assert.False(decoded);
        Assert.Equal(TimeSpan.Zero, value);
    }

    [Fact]
    public void ShouldRejectOutOfRangeDurationGivenDurationWhenEncodedAsUInt32Seconds()
    {
        // Arrange
        var value = TimeSpan.FromSeconds((double)uint.MaxValue + 1);

        // Act
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => WireDuration.ToSecondsUInt32(value, "wait"));

        // Assert
        Assert.Equal("wait", error.ParamName);
    }
}
