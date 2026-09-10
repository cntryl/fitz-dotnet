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
    public async Task ShouldPassExactAdmissionAuthorityGivenImmediateAcquire(byte responseType)
    {
        LeaseAuthority? observed = null;
        using var leaseClient = new LeaseClient((messageType, _, _) =>
        {
            return Task.FromResult(messageType == MessageTypes.LeaseAcquire
                ? AcquireResponse(responseType, 77)
                : SuccessResponse());
        });

        var result = await leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            30,
            (authority, cancellationToken) =>
            {
                observed = authority;
                Assert.False(cancellationToken.IsCancellationRequested);
                return ValueTask.FromResult("completed");
            });

        Assert.Equal("completed", result);
        Assert.Equal((ulong)77, observed?.FencingToken);
    }

    [Fact]
    public async Task ShouldPassFinalAuthorityGivenQueuedAcquire()
    {
        Action<byte[]>? acquireHandler = null;
        LeaseAuthority? observed = null;
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
    public async Task ShouldKeepAdmissionAuthorityStableGivenRenewalRotatesLiveToken()
    {
        var renewalCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ulong? releaseToken = null;
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

        var observed = await leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            1,
            async (authority, cancellationToken) =>
            {
                await renewalCompleted.Task.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);
                return authority;
            });

        Assert.Equal((ulong)77, observed.FencingToken);
        Assert.Equal((ulong)78, releaseToken);
    }

    [Fact]
    public async Task ShouldNotInvokeAuthorityCallbackGivenFailedAcquire()
    {
        var callbackInvoked = false;
        using var leaseClient = new LeaseClient((_, _, _) =>
            Task.FromResult(ErrorResponse(5001, "held by another owner")));

        var act = () => leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            30,
            (_, _) =>
            {
                callbackInvoked = true;
                return ValueTask.CompletedTask;
            });

        await Assert.ThrowsAsync<LeaseException>(act);
        Assert.False(callbackInvoked);
    }

    [Fact]
    public async Task ShouldNotInvokeAuthorityCallbackGivenQueuedAcquireTimesOut()
    {
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
        await Task.Yield();

        Assert.NotNull(acquireHandler);
        acquireHandler(ErrorResponse(5006, "lease wait timed out"));

        var error = await Assert.ThrowsAsync<LeaseException>(() => pending);
        Assert.Equal((uint)5006, error.DomainCode);
        Assert.False(callbackInvoked);
    }

    [Fact]
    public async Task ShouldNotInvokeAuthorityCallbackGivenQueuedAcquireIsCancelled()
    {
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

        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.False(callbackInvoked);
    }

    [Fact]
    public async Task ShouldPreserveOneArgumentManagedLeaseCallbacks()
    {
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
            cancellationToken => ValueTask.FromResult(!cancellationToken.IsCancellationRequested));
        await leaseClient.WithLeaseAsync(
            "lease://prod/app/non-generic",
            30,
            cancellationToken =>
            {
                nonGenericInvoked = !cancellationToken.IsCancellationRequested;
                return ValueTask.CompletedTask;
            });

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
