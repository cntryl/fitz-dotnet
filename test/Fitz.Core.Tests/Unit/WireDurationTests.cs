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
        Assert.Equal(value, WireDuration.FromSeconds(encoded));
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
    public void ShouldRejectNegativeDurationGivenDurationWhenEncodedAsMilliseconds()
    {
        // Arrange
        var value = TimeSpan.FromMilliseconds(-1);

        // Act
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => WireDuration.ToMilliseconds(value, "delay"));

        // Assert
        Assert.Equal("delay", error.ParamName);
    }

    [Fact]
    public void ShouldAcceptSubSecondPrecisionGivenDurationWhenEncodedAsMilliseconds()
    {
        // Arrange
        var value = TimeSpan.FromMilliseconds(1_500);

        // Act
        var encoded = WireDuration.ToMilliseconds(value, "delay");

        // Assert
        Assert.Equal(1_500, encoded);
    }

    [Fact]
    public void ShouldRejectSubMillisecondPrecisionGivenDurationWhenEncodedAsMilliseconds()
    {
        // Arrange
        var value = TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond + 1);

        // Act
        var error = Assert.Throws<ArgumentOutOfRangeException>(() => WireDuration.ToMilliseconds(value, "delay"));

        // Assert
        Assert.Equal("delay", error.ParamName);
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
