using Cntryl.Fitz.Domains.Lease;
using Cntryl.Fitz.Errors;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Core.Tests.Unit;

public sealed class ManagedLeaseLifecycleTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task should_cancel_callback_promptly_and_skip_renewal_and_release_given_disconnect(
        bool authorityAware)
    {
        var disconnects = new DisconnectRegistry();
        var renewCalls = 0;
        var releaseCalls = 0;
        using var leaseClient = new LeaseClient(
            (messageType, _, _) =>
            {
                if (messageType == MessageTypes.LeaseAcquire)
                {
                    return ValueTask.FromResult<ReadOnlyMemory<byte>>(AcquireResponse(77));
                }

                if (messageType == MessageTypes.LeaseRenew)
                {
                    Interlocked.Increment(ref renewCalls);
                }
                else if (messageType == MessageTypes.LeaseRelease)
                {
                    Interlocked.Increment(ref releaseCalls);
                }

                return ValueTask.FromResult<ReadOnlyMemory<byte>>(SuccessResponse());
            },
            registerOnDisconnect: disconnects.Register);
        using var parentCancellation = new CancellationTokenSource();
        var callbackStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackSettled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async ValueTask RunCallback(CancellationToken cancellationToken)
        {
            callbackStarted.TrySetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            finally
            {
                callbackSettled.TrySetResult();
            }
        }

        var pending = authorityAware
            ? leaseClient.WithLeaseAsync(
                "lease://prod/app/lock",
                300,
                (_, cancellationToken) => RunCallback(cancellationToken),
                ct: parentCancellation.Token)
            : leaseClient.WithLeaseAsync(
                "lease://prod/app/lock",
                300,
                RunCallback,
                ct: parentCancellation.Token);
        await callbackStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.Equal(1, disconnects.Count);

        disconnects.SignalDisconnect();

        try
        {
            var error = await Assert.ThrowsAsync<LeaseException>(
                () => pending.WaitAsync(TimeSpan.FromSeconds(1)));
            Assert.Equal("LEASE_LOST", error.Code);
        }
        finally
        {
            if (!pending.IsCompleted)
            {
                await parentCancellation.CancelAsync();
                try
                {
                    await pending;
                }
                catch (OperationCanceledException)
                {
                }
                catch (LeaseException)
                {
                }
                catch (AggregateException)
                {
                }
            }
        }

        Assert.True(callbackSettled.Task.IsCompleted);
        Assert.Equal(0, Volatile.Read(ref renewCalls));
        Assert.Equal(0, Volatile.Read(ref releaseCalls));
        Assert.Equal(0, disconnects.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task should_cancel_callback_during_synchronous_pre_await_work_given_disconnect(
        bool authorityAware)
    {
        var disconnects = new DisconnectRegistry();
        using var leaseClient = new LeaseClient(
            (messageType, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(
                messageType == MessageTypes.LeaseAcquire
                    ? AcquireResponse(77)
                    : SuccessResponse()),
            registerOnDisconnect: disconnects.Register);
        using var parentCancellation = new CancellationTokenSource();
        var callbackStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackSettled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async ValueTask RunCallback(CancellationToken cancellationToken)
        {
            callbackStarted.TrySetResult();
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    Thread.SpinWait(64);
                }

                await Task.Yield();
                cancellationToken.ThrowIfCancellationRequested();
            }
            finally
            {
                callbackSettled.TrySetResult();
            }
        }

        var pending = Task.Run(async () =>
        {
            if (authorityAware)
            {
                await leaseClient.WithLeaseAsync(
                    "lease://prod/app/lock",
                    300,
                    (_, cancellationToken) => RunCallback(cancellationToken),
                    ct: parentCancellation.Token);
            }
            else
            {
                await leaseClient.WithLeaseAsync(
                    "lease://prod/app/lock",
                    300,
                    RunCallback,
                    ct: parentCancellation.Token);
            }
        });
        await callbackStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        disconnects.SignalDisconnect();

        try
        {
            var error = await Assert.ThrowsAsync<LeaseException>(
                () => pending.WaitAsync(TimeSpan.FromSeconds(1)));
            Assert.Equal("LEASE_LOST", error.Code);
        }
        finally
        {
            if (!pending.IsCompleted)
            {
                await parentCancellation.CancelAsync();
                try
                {
                    await pending;
                }
                catch (OperationCanceledException)
                {
                }
                catch (LeaseException)
                {
                }
                catch (AggregateException)
                {
                }
            }
        }

        Assert.True(callbackSettled.Task.IsCompleted);
        Assert.Equal(0, disconnects.Count);
    }

    [Fact]
    public async Task should_dispose_registration_given_disconnect_during_registration()
    {
        var registrationDisposals = 0;
        using var leaseClient = new LeaseClient(
            (messageType, _, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(
                messageType == MessageTypes.LeaseAcquire
                    ? AcquireResponse(77)
                    : SuccessResponse()),
            registerOnDisconnect: listener =>
            {
                listener();
                return new TestRegistration(() => Interlocked.Increment(ref registrationDisposals));
            });

        await using var lease = await leaseClient.AcquireAsync("lease://prod/app/lock", 30);

        Assert.Equal(1, Volatile.Read(ref registrationDisposals));
    }

    [Fact]
    public async Task should_aggregate_lease_loss_and_cancellation_hook_failure_after_callback_settles()
    {
        var disconnects = new DisconnectRegistry();
        var renewalFailure = new InvalidOperationException("renewal failed");
        var cancellationHookFailure = new InvalidOperationException("cancellation hook failed");
        var renewCalls = 0;
        var releaseCalls = 0;
        using var leaseClient = new LeaseClient(
            (messageType, _, _) =>
            {
                if (messageType == MessageTypes.LeaseAcquire)
                {
                    return ValueTask.FromResult<ReadOnlyMemory<byte>>(AcquireResponse(77));
                }

                if (messageType == MessageTypes.LeaseRenew)
                {
                    Interlocked.Increment(ref renewCalls);
                    return ValueTask.FromException<ReadOnlyMemory<byte>>(renewalFailure);
                }

                Interlocked.Increment(ref releaseCalls);
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(SuccessResponse());
            },
            registerOnDisconnect: disconnects.Register);
        var callbackStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackCancellationObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callbackSettled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pending = leaseClient.WithLeaseAsync(
            "lease://prod/app/lock",
            1,
            async (_, cancellationToken) =>
            {
                using var registration = cancellationToken.Register(() =>
                {
                    callbackCancellationObserved.TrySetResult();
                    throw cancellationHookFailure;
                });
                callbackStarted.TrySetResult();
                try
                {
                    await callbackCancellationObserved.Task;
                }
                finally
                {
                    callbackSettled.TrySetResult();
                }
            });
        await callbackStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var error = await Assert.ThrowsAsync<AggregateException>(
            () => pending.WaitAsync(TimeSpan.FromSeconds(2)));
        var failures = error.Flatten().InnerExceptions;

        var leaseLoss = Assert.Single(failures.OfType<LeaseException>());
        Assert.Equal("LEASE_LOST", leaseLoss.Code);
        Assert.Same(renewalFailure, leaseLoss.InnerException);
        Assert.Contains(cancellationHookFailure, failures);
        Assert.True(callbackSettled.Task.IsCompleted);
        Assert.Equal(1, Volatile.Read(ref renewCalls));
        Assert.Equal(0, Volatile.Read(ref releaseCalls));
        Assert.Equal(0, disconnects.Count);
    }

    private static byte[] AcquireResponse(ulong token)
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU8(0);
        writer.WriteU8(0);
        writer.WriteU64(token);
        return writer.Build();
    }

    private static byte[] SuccessResponse() => [0];

    private sealed class DisconnectRegistry
    {
        private readonly object _gate = new();
        private readonly Dictionary<long, Action> _listeners = new();
        private long _nextId;

        internal int Count
        {
            get
            {
                lock (_gate)
                {
                    return _listeners.Count;
                }
            }
        }

        internal TestRegistration Register(Action listener)
        {
            long id;
            lock (_gate)
            {
                id = ++_nextId;
                _listeners.Add(id, listener);
            }

            return new TestRegistration(() =>
            {
                lock (_gate)
                {
                    _listeners.Remove(id);
                }
            });
        }

        internal void SignalDisconnect()
        {
            Action[] listeners;
            lock (_gate)
            {
                listeners = _listeners.Values.ToArray();
            }

            foreach (var listener in listeners)
            {
                listener();
            }
        }
    }
}
