using Cntryl.Fitz.Domains.Lease;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class LeaseRenewalScheduleTests
{
    [Theory]
    [InlineData(30, 10, 5)]
    [InlineData(9, 3, 3)]
    [InlineData(3, 1, 2)]
    public void ShouldRenewThreeTimesPerTermWithBoundedBudgetGivenTermWhenScheduling(int termSeconds, int intervalSeconds, int budgetSeconds)
    {
        // Arrange
        var term = TimeSpan.FromSeconds(termSeconds);

        // Act
        var schedule = LeaseRenewalSchedule.For(term);

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(intervalSeconds), schedule.Interval);
        Assert.Equal(TimeSpan.FromSeconds(budgetSeconds), schedule.Budget);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(14, true)]
    [InlineData(15, false)]
    public void ShouldAllowRetryOnlyWhileAttemptFitsBeforeExpiryGivenElapsedTimeWhenRenewalRejected(int elapsedSeconds, bool expected)
    {
        // Arrange
        var schedule = LeaseRenewalSchedule.For(TimeSpan.FromSeconds(30));

        // Act
        var canRetry = schedule.CanRetry(TimeSpan.FromSeconds(elapsedSeconds));

        // Assert
        Assert.Equal(expected, canRetry);
    }
}
