using Cntryl.Fitz.Abstractions.Domains.Lease;
using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Errors;
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
        using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            return Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(responseType, 77)
                : SuccessResponse());
        });


        // Assert
        var result = await leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            30,
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
        // Arrange
        Action<byte[]>? acquireHandler = null;

        // Act
        LeaseAuthority? observed = null;

        // Assert
        using var leaseClient = new LeaseClient(
            (messageType, _, _) => Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(2, 13)
                : SuccessResponse()),
            (messageType, handler) =>
            {
                Assert.Equal(MessageTypes.LeaseAcquire, messageType);
                acquireHandler = handler;
                return new TestRegistration();
            });

        var pending = leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            30,
            (authority, _) =>
            {
                observed = authority;
                return ValueTask.CompletedTask;
            },
            new LeaseExecutionOptions { WaitForAvailability = true, WaitSeconds = 5 });
        await Task.Yield();

        Assert.NotNull(acquireHandler);
        acquireHandler(AcquireResponse(0, 91));
        await pending;

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
        using var leaseClient = new LeaseClient((messageType, payload, _) =>
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
            1,
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
        using var leaseClient = new LeaseClient((_, _, _) =>
            Task.FromResult(ErrorResponse(5001, "held by another owner")));


        // Act
        var act = () => leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            30,
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
        // Arrange
        Action<byte[]>? acquireHandler = null;
        var callbackInvoked = false;
        using var leaseClient = new LeaseClient(
            (messageType, _, _) => Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(2, 13)
                : SuccessResponse()),
            (_, handler) =>
            {
                acquireHandler = handler;
                return new TestRegistration();
            });

        var pending = leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            30,
            (_, _) =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            },
            new LeaseExecutionOptions { WaitForAvailability = true, WaitSeconds = 1 });

        // Act
        await Task.Yield();


        // Assert
        Assert.NotNull(acquireHandler);
        acquireHandler(ErrorResponse(5006, "lease wait timed out"));

        var error = await Assert.ThrowsAsync<LeaseException>(() => pending);
        Assert.Equal((uint)5006, error.DomainCode);
        Assert.False(callbackInvoked);
    }

    [Fact]
    public async Task ShouldNotInvokeAuthorityCallbackGivenQueuedAcquireIsCancelledWhenManagedLeaseRuns()
    {
        // Arrange
        var callbackInvoked = false;
        using var cancellation = new CancellationTokenSource();
        using var leaseClient = new LeaseClient(
            (messageType, _, _) => Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(2, 13)
                : SuccessResponse()),
            (_, _) => new TestRegistration());

        var pending = leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            30,
            (_, _) =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            },
            new LeaseExecutionOptions { WaitForAvailability = true, WaitSeconds = 5 },
            cancellation.Token);
        await Task.Yield();


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
        using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            return Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(0, ++acquireCount)
                : SuccessResponse());
        });

        var result = await leaseClient.WithLeaseAsync(
            "lease://prod/app/generic",
            30,
            ct => ValueTask.FromResult(!ct.IsCancellationRequested));

        // Act
        await leaseClient.WithLeaseAsync(
            "lease://prod/app/non-generic",
            30,
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
        writer.WriteU32(code);
        writer.WriteString(message);
        return writer.Build();
    }

    static byte[] SuccessResponse() => [0];
}
