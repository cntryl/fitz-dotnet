using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using Cntryl.Fitz.Abstractions.Domains.Lease;

namespace Cntryl.Fitz.Domains.Lease;

/// <summary>
/// Race-safe Lease inventory observer. See <see cref="ILeaseInventoryObserver"/> for the
/// guarantees this provides.
/// </summary>
internal sealed class LeaseInventoryObserver : ILeaseInventoryObserver
{
    private readonly LeaseClient _client;
    private readonly string _pattern;
    private readonly LeaseObserveOptions _options;
    private readonly object _gate = new();
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly Channel<LeaseInventoryUpdate> _updates;
    private readonly CancellationTokenSource _lifetimeCts = new();

    private IDisposable? _reconnectRegistration;
    private LeaseSubscription? _subscription;
    private CancellationTokenSource? _consumerLoopCts;
    private Task? _consumerLoopTask;
    private Task? _reconciliationLoopTask;
    private Task? _subscriptionRecoveryTask;
    private bool _subscriptionRecoveryRequested;
    private long _subscriptionGeneration;
    private volatile IReadOnlyDictionary<string, LeaseListItem> _view =
        new Dictionary<string, LeaseListItem>(StringComparer.Ordinal);
    private volatile bool _isReady;
    private bool _bootstrapDirty;
    private readonly HashSet<string> _pendingRoutes = new(StringComparer.Ordinal);
    private int _disposed;

    internal LeaseInventoryObserver(LeaseClient client, string pattern, LeaseObserveOptions options)
    {
        _client = client;
        _pattern = pattern;
        _options = options;
        _updates = Channel.CreateBounded<LeaseInventoryUpdate>(new BoundedChannelOptions(
            Math.Max(1, options.UpdateBufferCapacity))
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = false,
            SingleWriter = false,
        });
    }

    public IReadOnlyDictionary<string, LeaseListItem> View => _view;

    public bool IsReady => _isReady;

    public IAsyncEnumerable<LeaseInventoryUpdate> Updates => _updates.Reader.ReadAllAsync();

    internal async Task StartAsync(CancellationToken ct)
    {
        _reconnectRegistration = _client.RegisterReconnectListener(HandleReconnectAsync);
        try
        {
            await BootstrapAsync(ct).ConfigureAwait(false);
            _reconciliationLoopTask = Task.Run(() => ReconciliationLoopAsync(_lifetimeCts.Token), CancellationToken.None);
        }
        catch
        {
            await DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Disposed via the outer using(linked) block; the analyzer cannot follow the try/catch guard around a possibly-already-disposed _lifetimeCts.")]
    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Reconnect recovery retries transient transport, broker, and LIST failures through the bounded subscription-recovery loop.")]
    private async ValueTask HandleReconnectAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        // _lifetimeCts may already be disposed if DisposeAsync races past its own disposed
        // check between here and the CancellationTokenSource.Dispose() call at the end of
        // teardown; treat that exactly like the disposed check above.
        CancellationTokenSource linked;
        try
        {
            linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _lifetimeCts.Token);
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        using (linked)
        {
            lock (_gate)
            {
                _isReady = false;
                _bootstrapDirty = true;
                _subscriptionGeneration++;
            }

            try
            {
                // Threading the lifetime token in means DisposeAsync's cancellation actually
                // stops an in-flight reconnect bootstrap rather than racing it unsynchronized.
                await BootstrapAsync(linked.Token, isReconnect: true).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (Volatile.Read(ref _disposed) != 0)
            {
                // Disposed concurrently with a reconnect-triggered bootstrap; DisposeAsync
                // already tears down the subscription and background consumer loop itself.
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested || _lifetimeCts.IsCancellationRequested)
            {
                // The reconnect attempt or observer lifetime ended; there is no live
                // recovery target for a background retry.
            }
            catch
            {
                // The restored wire subscription may still be healthy, but a failed LIST
                // leaves the view knowingly stale. Hand recovery to the bounded retry loop;
                // replacing the subscription also closes any ambiguity about the restored
                // registration after a transport failure.
                RequestSubscriptionRecovery();
            }
        }
    }

    private async Task BootstrapAsync(CancellationToken ct, bool isReconnect = false)
    {
        await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // A disposed observer's bootstrap must abort cleanly instead of proceeding to
            // mutate state that DisposeAsync is about to (or already did) tear down.
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            lock (_gate)
            {
                _isReady = false;
                _bootstrapDirty = false;
                _pendingRoutes.Clear();
            }

            // On a reconnect, LeaseClient.HandleReconnect already restored the wire-level
            // subscription (RestoreSubscriptionsAsync) for this observer's route before this
            // listener ran, reusing the same registration/buffer that _subscription reads
            // from. Tearing it down and re-subscribing here would just add a redundant
            // UNSUBSCRIBE+SUBSCRIBE round trip pair.
            if (!isReconnect || _subscription is null)
            {
                await StopConsumerLoopAsync().ConfigureAwait(false);
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                    _subscription = null;
                }

                // 1. Subscribe and wait for the acknowledgement (subscription id).
                var subscription = await _client.SubscribeObserverAsync(
                    _pattern,
                    InvalidateNotification,
                    ct).ConfigureAwait(false);
                _subscription = subscription;
                lock (_gate)
                {
                    _subscriptionGeneration++;
                }

                // 2. Start buffering incoming notifications for this subscription; they are not
                // applied until one LIST pass completes without a concurrent invalidation.
                var consumerLoopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
                _consumerLoopCts = consumerLoopCts;
                _consumerLoopTask = Task.Run(() => ConsumeNotificationsAsync(subscription, consumerLoopCts.Token), CancellationToken.None);
            }

            // 3-5. LIST to completion and install only after a complete pass
            // observed no buffered invalidation. Repeating instead of
            // marking ready before the drain closes the acknowledgement /
            // pagination race for an arbitrarily long bootstrap.
            while (true)
            {
                long passSubscriptionGeneration;
                lock (_gate)
                {
                    passSubscriptionGeneration = _subscriptionGeneration;
                }

                var freshView = await ListAllAsync(ct).ConfigureAwait(false);
                lock (_gate)
                {
                    // DisposeAsync can run concurrently while this LIST pass is in flight (it
                    // blocks on _refreshGate until this method returns); never install a view
                    // or mark ready once disposal has begun.
                    if (Volatile.Read(ref _disposed) != 0)
                    {
                        return;
                    }

                    if (_subscriptionGeneration != passSubscriptionGeneration)
                    {
                        // A newer reconnect or subscription-recovery request
                        // superseded this candidate while LIST was in flight.
                        // Leave readiness false and release _refreshGate so
                        // that already-coalesced bootstrap can run next;
                        // throwing here would escape the reconnect listener.
                        return;
                    }

                    if (_bootstrapDirty)
                    {
                        _bootstrapDirty = false;
                        continue;
                    }

                    _view = freshView;
                    _isReady = true;
                    break;
                }
            }
        }
        finally
        {
            _refreshGate.Release();
        }
    }

    private void InvalidateNotification(LeaseChangeEvent change)
    {
        lock (_gate)
        {
            if (!_isReady)
            {
                _bootstrapDirty = true;
            }
            else
            {
                _pendingRoutes.Add(change.Route);
            }
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "A terminated subscription is recovered with a new subscribe-before-list bootstrap regardless of its terminal error type.")]
    private async Task ConsumeNotificationsAsync(LeaseSubscription subscription, CancellationToken ct)
    {
        try
        {
            await foreach (var change in subscription.WithCancellation(ct))
            {
                bool ready;
                lock (_gate)
                {
                    ready = _isReady;
                    if (!ready)
                    {
                        _bootstrapDirty = true;
                    }
                }

                if (!ready)
                {
                    continue;
                }

                try
                {
                    await HandleSteadyStateNotificationAsync(ct).ConfigureAwait(false);
                }
                catch
                {
                    // Best effort: a missed update is corrected by the periodic reconciliation.
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose/rebootstrap.
            return;
        }
        catch
        {
            // Handler overflow/backpressure removes or invalidates the wire
            // registration. Periodic LIST-only reconciliation cannot repair
            // that gap, so immediately replace the subscription and rebuild
            // the view. The recovery loop owns retry/backoff for transient
            // SUBSCRIBE or LIST failures.
        }

        RequestSubscriptionRecovery();
    }

    private void RequestSubscriptionRecovery()
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            _isReady = false;
            _bootstrapDirty = true;
            _subscriptionGeneration++;
            _subscriptionRecoveryRequested = true;
            if (_subscriptionRecoveryTask is null || _subscriptionRecoveryTask.IsCompleted)
            {
                _subscriptionRecoveryTask = Task.Run(RecoverSubscriptionLoopAsync, CancellationToken.None);
            }
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Subscription recovery retries transient transport, broker, and LIST failures until disposal cancels the observer lifetime.")]
    private async Task RecoverSubscriptionLoopAsync()
    {
        var attempt = 0;
        while (Volatile.Read(ref _disposed) == 0)
        {
            lock (_gate)
            {
                _subscriptionRecoveryRequested = false;
            }

            try
            {
                await BootstrapAsync(_lifetimeCts.Token).ConfigureAwait(false);
                attempt = 0;
            }
            catch (OperationCanceledException) when (_lifetimeCts.IsCancellationRequested)
            {
                return;
            }
            catch
            {
                attempt++;
                await Task.Delay(NextSubscriptionRecoveryDelay(attempt), _lifetimeCts.Token).ConfigureAwait(false);
                continue;
            }

            lock (_gate)
            {
                if (_subscriptionRecoveryRequested)
                {
                    continue;
                }

                _subscriptionRecoveryTask = null;
                return;
            }
        }
    }

    [SuppressMessage("Security", "CA5394:DoNotUseInsecureRandomness", Justification = "Jitter for subscription-recovery timing is not security-sensitive.")]
    private static TimeSpan NextSubscriptionRecoveryDelay(int attempt)
    {
        const double baseMilliseconds = 50;
        const double maximumMilliseconds = 2_000;
        var exponent = Math.Min(Math.Max(attempt - 1, 0), 6);
        var capped = Math.Min(maximumMilliseconds, baseMilliseconds * Math.Pow(2, exponent));
        var jittered = capped * (0.8 + (Random.Shared.NextDouble() * 0.4));
        return TimeSpan.FromMilliseconds(Math.Max(baseMilliseconds, jittered));
    }

    private async Task HandleSteadyStateNotificationAsync(CancellationToken ct)
    {
        await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
        var updates = new List<LeaseInventoryUpdate>();
        try
        {
            var affectedRoutes = new HashSet<string>(StringComparer.Ordinal);
            while (true)
            {
                lock (_gate)
                {
                    if (!_isReady || _pendingRoutes.Count == 0)
                    {
                        return;
                    }

                    affectedRoutes.UnionWith(_pendingRoutes);
                    _pendingRoutes.Clear();
                }

                var refreshed = await ListAllAsync(ct).ConfigureAwait(false);
                lock (_gate)
                {
                    // A reconnect can invalidate readiness while this relist
                    // is in flight. A notification that raced the pass is
                    // coalesced into another pass before anything is exposed.
                    if (!_isReady)
                    {
                        return;
                    }

                    if (_pendingRoutes.Count > 0)
                    {
                        continue;
                    }

                    _view = refreshed;
                    foreach (var route in affectedRoutes)
                    {
                        refreshed.TryGetValue(route, out var item);
                        updates.Add(new LeaseInventoryUpdate(route, item));
                    }
                    break;
                }
            }
        }
        finally
        {
            _refreshGate.Release();
        }

        foreach (var update in updates)
        {
            await _updates.Writer.WriteAsync(update, ct).ConfigureAwait(false);
        }
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Best-effort background loop; a failed reconciliation is retried on the next interval.")]
    private async Task ReconciliationLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var delay = NextReconciliationDelay(_options);
            try
            {
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (ct.IsCancellationRequested)
            {
                return;
            }

            try
            {
                await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    var relisted = await ListAllAsync(ct).ConfigureAwait(false);
                    lock (_gate)
                    {
                        if (_isReady && _pendingRoutes.Count == 0)
                        {
                            _view = relisted;
                        }
                    }
                }
                finally
                {
                    _refreshGate.Release();
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch
            {
                // Best effort: try again on the next interval.
            }
        }
    }

    [SuppressMessage("Security", "CA5394:DoNotUseInsecureRandomness", Justification = "Jitter for reconciliation timing is not security-sensitive.")]
    private static TimeSpan NextReconciliationDelay(LeaseObserveOptions options)
    {
        var jitterRatio = options.ReconciliationJitterRatio;
        if (jitterRatio <= 0)
        {
            return options.ReconciliationInterval;
        }

        var factor = 1.0 + (((Random.Shared.NextDouble() * 2) - 1) * jitterRatio);
        var ticks = (long)(options.ReconciliationInterval.Ticks * factor);
        return TimeSpan.FromTicks(Math.Max(ticks, 0));
    }

    private async Task<Dictionary<string, LeaseListItem>> ListAllAsync(CancellationToken ct)
    {
        var result = new Dictionary<string, LeaseListItem>(StringComparer.Ordinal);
        LeaseListCursor? cursor = null;
        do
        {
            var page = await _client.ListAsync(_pattern, cursor, limit: null, ct).ConfigureAwait(false);
            foreach (var item in page.Items)
            {
                result[item.Route] = item;
            }

            cursor = page.NextCursor;
        } while (cursor is not null);

        return result;
    }

    private async Task StopConsumerLoopAsync()
    {
        var cts = _consumerLoopCts;
        var task = _consumerLoopTask;
        _consumerLoopCts = null;
        _consumerLoopTask = null;

        if (cts is null)
        {
            return;
        }

        await cts.CancelAsync().ConfigureAwait(false);
        try
        {
            if (task is not null)
            {
                await task.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected.
        }
        finally
        {
            cts.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _reconnectRegistration?.Dispose();
        await _lifetimeCts.CancelAsync().ConfigureAwait(false);

        if (_reconciliationLoopTask is not null)
        {
            try
            {
                await _reconciliationLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected.
            }
        }

        Task? subscriptionRecoveryTask;
        lock (_gate)
        {
            subscriptionRecoveryTask = _subscriptionRecoveryTask;
        }
        if (subscriptionRecoveryTask is not null)
        {
            try
            {
                await subscriptionRecoveryTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected after lifetime cancellation.
            }
        }

        // _disposed is already set above, so no new BootstrapAsync/HandleReconnectAsync call can
        // start past this point (both check it before ever touching _refreshGate). Acquiring the
        // gate here therefore just waits out one that is already in flight - once acquired, it is
        // safe to tear down (and, at the very end, dispose) the shared state without racing it.
        await _refreshGate.WaitAsync().ConfigureAwait(false);
        try
        {
            await StopConsumerLoopAsync().ConfigureAwait(false);

            if (_subscription is not null)
            {
                await _subscription.DisposeAsync().ConfigureAwait(false);
                _subscription = null;
            }
        }
        finally
        {
            _refreshGate.Release();
        }

        _updates.Writer.TryComplete();
        _lifetimeCts.Dispose();
        _refreshGate.Dispose();
    }
}
