using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ObservabilityTests
{
    [Theory]
    [InlineData(-0.1)]
    [InlineData(1.1)]
    [InlineData(double.NaN)]
    public void ShouldRejectInvalidFractionGivenPercentileLookupWhenRecordingMetric(double percentile)
    {
        // Arrange
        var histogram = new LatencyHistogram();

        // Act
        histogram.Record(10);


        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => histogram.GetPercentile(percentile));
    }

    [Fact]
    public void ShouldRejectNegativeDurationGivenLatencySampleWhenRecordingMetric()
    {
        // Arrange
        // Act
        // Assert
        var histogram = new LatencyHistogram();

        Assert.Throws<ArgumentOutOfRangeException>(() => histogram.Record(-1));
        Assert.Equal(0, histogram.Count);
    }

    [Fact]
    public void ShouldRejectNegativeCountGivenThroughputSampleWhenRecordingMetric()
    {
        // Arrange
        // Act
        // Assert
        var meter = new ThroughputMeter();

        Assert.Throws<ArgumentOutOfRangeException>(() => meter.RecordOperations(-1));
        Assert.Equal(0, meter.TotalOperations);
    }
}
