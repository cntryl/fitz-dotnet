using System.Runtime.ExceptionServices;

namespace Cntryl.Fitz.Domains.Lease;

/// <summary>
/// Runs a callback while holding a lease: renews the lease in the background, cancels the
/// callback's token when ownership is lost, and releases the lease when the callback ends.
/// </summary>
/// <remarks>
/// Failures are reported in priority order: caller cancellation, the callback's own error, lease
/// loss (<c>LEASE_LOST</c>), a cancellation-callback failure, then a release failure.
/// </remarks>
static class LeaseScope
{
    static readonly TimeSpan ReleaseBudget = TimeSpan.FromSeconds(5);

    /// <summary>Runs <paramref name="callback"/> under <paramref name="lease"/>, then disposes the lease.</summary>
    /// <typeparam name="T">The callback's result type.</typeparam>
    /// <param name="lease">A freshly acquired lease, owned by the scope from here on.</param>
    /// <param name="ttl">The lease term each renewal requests.</param>
    /// <param name="callback">The work to run while the lease is held.</param>
    /// <param name="time">The clock that schedules renewals and bounds each renewal and the release.</param>
    /// <param name="ct">The caller's cancellation token.</param>
    /// <returns>The callback's result.</returns>
    internal static async Task<T> RunAsync<T>(
        LeaseHandle lease,
        TimeSpan ttl,
        Func<LeaseAuthority, CancellationToken, ValueTask<T>> callback,
        TimeProvider time,
        CancellationToken ct)
    {
        await using var leaseScope = lease.ConfigureAwait(false);
        using var lifecycle = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // Connection loss must cancel the callback even while it runs synchronously, so a
        // watcher on the thread pool cancels the token rather than the renewal loop below.
        using var connectionLossWatch = new ConnectionLossWatch(lease.ConnectionLost, lifecycle);
        var callbackTask = Start(callback, new LeaseAuthority(lease.FencingToken), lifecycle.Token);

        var (leaseLoss, cancellationError) = await KeepRenewedAsync(lease, ttl, callbackTask, lifecycle, time, ct).ConfigureAwait(false);
        var watchCancellationError = await connectionLossWatch.StopAsync().ConfigureAwait(false);
        if (leaseLoss is null && lease.ConnectionLost.IsCompleted)
        {
            leaseLoss = LeaseLost();
            cancellationError = await CancelAsync(lifecycle).ConfigureAwait(false);
        }

        cancellationError ??= watchCancellationError;

        T? value = default;
        Exception? callbackError = null;
        try
        {
            value = await callbackTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (lifecycle.IsCancellationRequested)
        {
            // The underlying lifecycle cause is reported below.
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            callbackError = error;
        }

        var releaseError = leaseLoss is null ? await ReleaseAsync(lease, time).ConfigureAwait(false) : null;

        ct.ThrowIfCancellationRequested();
        if (callbackError is not null)
            ExceptionDispatchInfo.Capture(callbackError).Throw();
        if (leaseLoss is not null)
            throw leaseLoss;
        if (cancellationError is not null)
            ExceptionDispatchInfo.Capture(cancellationError).Throw();
        if (releaseError is not null)
            ExceptionDispatchInfo.Capture(releaseError).Throw();
        return value!;
    }

    static Task<T> Start<T>(Func<LeaseAuthority, CancellationToken, ValueTask<T>> callback, LeaseAuthority authority, CancellationToken ct)
    {
        try
        {
            return callback(authority, ct).AsTask();
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            return Task.FromException<T>(error);
        }
    }

    // Renews until the callback finishes, the caller cancels, or ownership is lost. On loss,
    // invalidates the handle and cancels the callback's token.
    static async Task<(LeaseException? LeaseLoss, Exception? CancellationError)> KeepRenewedAsync(
        LeaseHandle lease,
        TimeSpan ttl,
        Task callbackTask,
        CancellationTokenSource lifecycle,
        TimeProvider time,
        CancellationToken ct)
    {
        var schedule = LeaseRenewalSchedule.For(ttl);
        var lastRenewal = time.GetTimestamp();
        while (!callbackTask.IsCompleted)
        {
            var delay = Task.Delay(schedule.Interval, time, lifecycle.Token);
            var completed = await Task.WhenAny(callbackTask, lease.ConnectionLost, delay).ConfigureAwait(false);
            if (ct.IsCancellationRequested || lease.ConnectionLost.IsCompleted || completed == callbackTask)
                return default;

            try
            {
                using var budget = new CancellationTokenSource(schedule.Budget, time);
                using var renewalDeadline = CancellationTokenSource.CreateLinkedTokenSource(lifecycle.Token, budget.Token);
                await lease.ExtendAsync(ttl, renewalDeadline.Token).ConfigureAwait(false);
                lastRenewal = time.GetTimestamp();
                if (lease.FencingTokenChanged.IsCompleted)
                {
                    var rotation = new LeaseException(
                        "Lease renewal changed the callback's fencing authority token",
                        "FENCING_TOKEN_CHANGED");
                    return await LoseAsync(lease, lifecycle, rotation).ConfigureAwait(false);
                }
            }
            catch (LeaseException error) when (IsDefiniteRenewalRejection(error) &&
                schedule.CanRetry(time.GetElapsedTime(lastRenewal)))
            {
                // The broker did not apply the renewal and the lease still has slack: the handle
                // remains valid, so try again on the next interval.
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
                return await LoseAsync(lease, lifecycle, error).ConfigureAwait(false);
            }
        }

        return default;
    }

    static async Task<(LeaseException? LeaseLoss, Exception? CancellationError)> LoseAsync(
        LeaseHandle lease,
        CancellationTokenSource lifecycle,
        Exception cause)
    {
        lease.Invalidate();
        return (LeaseLost(cause), await CancelAsync(lifecycle).ConfigureAwait(false));
    }

    static async Task<Exception?> ReleaseAsync(LeaseHandle lease, TimeProvider time)
    {
        using var cleanup = new CancellationTokenSource(ReleaseBudget, time);
        try
        {
            await lease.ReleaseAsync(cleanup.Token).ConfigureAwait(false);
            return null;
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            return error;
        }
    }

    static LeaseException LeaseLost(Exception? cause = null) =>
        cause is null
            ? new LeaseException("Lease ownership was lost", "LEASE_LOST")
            : new LeaseException("Lease ownership was lost", "LEASE_LOST", cause);

    // Cancels the callback's token, returning a failure thrown by a registered cancellation callback.
    static async Task<Exception?> CancelAsync(CancellationTokenSource cancellation)
    {
        try
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            return null;
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            return error;
        }
    }

    // A rejection the broker reports as not applied and retryable leaves the handle open.
    static bool IsDefiniteRenewalRejection(LeaseException error) =>
        error is { Status: 1, ErrorEffect: 0 } && Retryability.IsRetryable(error);

    /// <summary>
    /// Cancels a callback's token from the thread pool when the connection drops, so a callback
    /// busy with synchronous work still observes the loss.
    /// </summary>
    sealed class ConnectionLossWatch : IDisposable
    {
        readonly CancellationTokenSource _stop = new();
        readonly Task<Exception?> _watch;

        internal ConnectionLossWatch(Task connectionLost, CancellationTokenSource lifecycle) =>
            _watch = WatchAsync(connectionLost, lifecycle, _stop.Token);

        /// <summary>Stops watching; the lifecycle source is untouched afterwards.</summary>
        /// <returns>A failure raised by a cancellation callback, if the watch cancelled the lifecycle.</returns>
        internal async Task<Exception?> StopAsync()
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            return await _watch.ConfigureAwait(false);
        }

        public void Dispose() => _stop.Dispose();

        static async Task<Exception?> WatchAsync(Task connectionLost, CancellationTokenSource lifecycle, CancellationToken stop)
        {
            try
            {
                await connectionLost.WaitAsync(stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return null;
            }

            return await CancelAsync(lifecycle).ConfigureAwait(false);
        }
    }
}

/// <summary>When a managed lease renews, and how long each renewal may take.</summary>
/// <param name="Term">The lease term each renewal requests.</param>
/// <param name="Interval">Time between renewals: a third of the term, so one missed renewal still leaves slack.</param>
/// <param name="Budget">Deadline for one renewal attempt.</param>
readonly record struct LeaseRenewalSchedule(TimeSpan Term, TimeSpan Interval, TimeSpan Budget)
{
    const int RenewalsPerTerm = 3;
    static readonly TimeSpan BudgetFloor = TimeSpan.FromSeconds(2);
    static readonly TimeSpan BudgetCap = TimeSpan.FromSeconds(5);

    /// <summary>Builds the schedule for a lease term.</summary>
    /// <param name="term">The lease term.</param>
    /// <returns>The schedule.</returns>
    internal static LeaseRenewalSchedule For(TimeSpan term)
    {
        var interval = term / RenewalsPerTerm;
        var budget = Min(BudgetCap, Min(Max(interval, BudgetFloor), term - interval));
        return new LeaseRenewalSchedule(term, interval, budget);
    }

    /// <summary>
    /// Whether another attempt can finish, budget included, before the last successful renewal expires.
    /// </summary>
    /// <param name="sinceLastRenewal">Time since the last successful renewal (or the acquisition).</param>
    /// <returns><see langword="true"/> when retrying on the next interval is still safe.</returns>
    internal bool CanRetry(TimeSpan sinceLastRenewal) => sinceLastRenewal + Interval + Budget < Term;

    static TimeSpan Min(TimeSpan left, TimeSpan right) => left <= right ? left : right;

    static TimeSpan Max(TimeSpan left, TimeSpan right) => left >= right ? left : right;
}
