using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class SessionLatchTests
{
    [Fact]
    public void ShouldCloseAndReleaseRegistrationGivenDisconnectDuringConstructionWhenCreated()
    {
        // Arrange
        using var registration = new TrackingRegistration();
        var lost = 0;

        // Act
        var latch = new SessionLatch(
            onDisconnect =>
            {
                onDisconnect();
                return registration;
            },
            () => lost++);

        // Assert
        Assert.True(latch.IsClosed);
        Assert.True(registration.Disposed);
        Assert.Equal(1, lost);
    }

    [Fact]
    public void ShouldNotReportConnectionLossGivenOwnerClosedFirstWhenDisconnected()
    {
        // Arrange
        Action? disconnect = null;
        using var registration = new TrackingRegistration();
        var lost = 0;
        var latch = new SessionLatch(
            onDisconnect =>
            {
                disconnect = onDisconnect;
                return registration;
            },
            () => lost++);

        // Act
        var closedByOwner = latch.TryClose();
        disconnect!();

        // Assert
        Assert.True(closedByOwner);
        Assert.False(latch.TryClose());
        Assert.True(registration.Disposed);
        Assert.Equal(0, lost);
    }

    sealed class TrackingRegistration : IDisposable
    {
        internal bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }
}
