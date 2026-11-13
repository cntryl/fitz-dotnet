using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ManagedLeaseAuthorityTests
{
    [Theory]
    [InlineData((byte)0)]
    [InlineData((byte)1)]
    public async Task ShouldPassExactAdmissionAuthorityGivenImmediateAcquireWhenManagedLeaseRuns(byte responseType)
    {
        // Arrange
        LeaseAuthority? observed = null;

        // Act
        await using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            return Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(responseType, 77)
                : SuccessResponse());
        });


        // Assert
        var result = await leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            TimeSpan.FromSeconds(30),
            (authority, ct) =>
            {
                observed = authority;
                Assert.False(ct.IsCancellationRequested);
                return ValueTask.FromResult("completed");
            });

        Assert.Equal("completed", result);
        Assert.Equal((ulong)77, observed?.FencingToken);
    }

    [Fact]
    public async Task ShouldPassFinalAuthorityGivenQueuedAcquireWhenManagedLeaseRuns()
    {
        // Arrange: the request completes only when the broker sends the final ACQUIRE frame.
        var finalFrame = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        LeaseAuthority? observed = null;
        await using var leaseClient = new LeaseClient(
            (messageType, _, _) => messageType == MessageTypes.LeaseAcquire
                ? finalFrame.Task
                : Task.FromResult(SuccessResponse()));
        var pending = leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            TimeSpan.FromSeconds(30),
            (authority, _) =>
            {
                observed = authority;
                return ValueTask.CompletedTask;
            },
            new LeaseExecutionOptions { WaitForAvailability = true, Wait = TimeSpan.FromSeconds(5) });

        // Act
        finalFrame.SetResult(AcquireResponse(0, 91));
        await pending.WaitAsync(TimeSpan.FromSeconds(5));

        // Assert
        Assert.Equal((ulong)91, observed?.FencingToken);
    }

    [Fact]
    public async Task ShouldSurfaceLeaseLossGivenRotatedAuthorityTokenWhenRenewing()
    {
        // Arrange
        var renewalCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Act
        ulong? releaseToken = null;

        // Assert
        await using var leaseClient = new LeaseClient((messageType, payload, _) =>
        {
            if (messageType == MessageTypes.LeaseAcquire)
            {
                return Task.FromResult(AcquireResponse(0, 77));
            }

            if (messageType == MessageTypes.LeaseRenew)
            {
                using var writer = new BinaryBufferWriter();
                writer.WriteU8(0);
                writer.WriteU64(78);
                renewalCompleted.TrySetResult();
                return Task.FromResult(writer.Build());
            }

            var reader = new BinaryBufferReader(payload);
            Assert.Equal("lease://prod/app/lock", reader.ReadString());
            Assert.Equal(string.Empty, reader.ReadString());
            releaseToken = reader.ReadU64();
            Assert.True(reader.IsEof);
            return Task.FromResult(SuccessResponse());
        });

        var error = await Assert.ThrowsAsync<LeaseException>(() => leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            TimeSpan.FromSeconds(1),
            async (authority, ct) =>
            {
                await renewalCompleted.Task.WaitAsync(TimeSpan.FromSeconds(2), ct);
                return authority;
            }));

        Assert.Equal("LEASE_LOST", error.Code);
        var rotation = Assert.IsType<LeaseException>(error.InnerException);
        Assert.Equal("FENCING_TOKEN_CHANGED", rotation.Code);
        Assert.Null(releaseToken);
    }

    [Fact]
    public async Task ShouldNotInvokeAuthorityCallbackGivenFailedAcquireWhenManagedLeaseRuns()
    {
        // Arrange
        var callbackInvoked = false;
        await using var leaseClient = new LeaseClient((_, _, _) =>
            Task.FromResult(ErrorResponse(5001, "held by another owner")));


        // Act
        var act = () => leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            TimeSpan.FromSeconds(30),
            (_, _) =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            });


        // Assert
        await Assert.ThrowsAsync<LeaseException>(act);
        Assert.False(callbackInvoked);
    }

    [Fact]
    public async Task ShouldNotInvokeAuthorityCallbackGivenQueuedAcquireTimesOutWhenManagedLeaseRuns()
    {
        // Arrange: the broker ends the queued wait with an error on the final ACQUIRE frame.
        var finalFrame = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackInvoked = false;
        await using var leaseClient = new LeaseClient(
            (messageType, _, _) => messageType == MessageTypes.LeaseAcquire
                ? finalFrame.Task
                : Task.FromResult(SuccessResponse()));
        var pending = leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            TimeSpan.FromSeconds(30),
            (_, _) =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            },
            new LeaseExecutionOptions { WaitForAvailability = true, Wait = TimeSpan.FromSeconds(1) });

        // Act
        finalFrame.SetResult(ErrorResponse(5055, "lease wait timed out"));

        // Assert
        var error = await Assert.ThrowsAsync<LeaseException>(() => pending);
        Assert.Equal((uint)5055, error.DomainCode);
        Assert.False(callbackInvoked);
    }

    [Fact]
    public async Task ShouldNotInvokeAuthorityCallbackGivenQueuedAcquireIsCancelledWhenManagedLeaseRuns()
    {
        // Arrange: no final ACQUIRE frame arrives before the caller cancels.
        var callbackInvoked = false;
        using var cancellation = new CancellationTokenSource();
        await using var leaseClient = new LeaseClient(
            async (messageType, _, ct) =>
            {
                if (messageType == MessageTypes.LeaseAcquire)
                    await Task.Delay(Timeout.Infinite, ct);
                return SuccessResponse();
            });
        var pending = leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            TimeSpan.FromSeconds(30),
            (_, _) =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            },
            new LeaseExecutionOptions { WaitForAvailability = true, Wait = TimeSpan.FromSeconds(5) },
            cancellation.Token);

        // Act
        await cancellation.CancelAsync();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.False(callbackInvoked);
    }

    [Fact]
    public async Task ShouldPreserveOneArgumentManagedLeaseCallbacksGivenLeaseAcquisitionWhenCallbackRuns()
    {
        // Arrange
        var acquireCount = 0UL;
        var nonGenericInvoked = false;
        await using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            return Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(0, ++acquireCount)
                : SuccessResponse());
        });

        var result = await leaseClient.WithLeaseAsync(
            "lease://prod/app/generic",
            TimeSpan.FromSeconds(30),
            ct => ValueTask.FromResult(!ct.IsCancellationRequested));

        // Act
        await leaseClient.WithLeaseAsync(
            "lease://prod/app/non-generic",
            TimeSpan.FromSeconds(30),
            ct =>
            {
                nonGenericInvoked = !ct.IsCancellationRequested;
                return ValueTask.CompletedTask;
            });


        // Assert
        Assert.True(result);
        Assert.True(nonGenericInvoked);
    }

    static byte[] AcquireResponse(byte responseType, ulong token)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        writer.WriteU8(responseType);
        writer.WriteU64(token);
        return writer.Build();
    }

    static byte[] ErrorResponse(uint code, string message)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(1);
        TestErrorEnvelope.Write(writer, code);
        writer.WriteString(message);
        return writer.Build();
    }

    static byte[] SuccessResponse() => [0];
}
