using System.Threading.Channels;
using Cntryl.Fitz.Runtime;

namespace Cntryl.Fitz.Domains.Lease;

/// <summary>
/// Race-safe Lease inventory observer. See <see cref="ILeaseInventoryObserver"/> for the
/// guarantees this provides.
/// </summary>
sealed class LeaseInventoryObserver : ILeaseInventoryObserver
{
    const int MaxImmediateConvergencePasses = 3;
    readonly LeaseClient _client;
    readonly string _pattern;
    readonly LeaseObserveOptions _options;
    readonly TimeProvider _time;
    readonly Lock _gate = new();
    readonly SemaphoreSlim _refreshGate = new(1, 1);
    readonly Channel<LeaseInventoryUpdate> _updates;
    readonly CancellationTokenSource _lifetimeCts = new();

    // Captured once: the source is cancelled before it is disposed, so this token stays
    // usable (and cancelled) by callbacks that race teardown.
    readonly CancellationToken _lifetime;

    IDisposable? _reconnectRegistration;
    LeaseSubscription? _subscription;
    CancellationTokenSource? _consumerLoopCts;
    Task? _consumerLoopTask;
    Task? _reconciliationLoopTask;
    Task? _subscriptionRecoveryTask;
    bool _subscriptionRecoveryRequested;
    long _subscriptionGeneration;
    volatile IReadOnlyDictionary<string, LeaseListItem> _view =
        new Dictionary<string, LeaseListItem>(StringComparer.Ordinal);
    volatile bool _isReady;
    bool _bootstrapDirty;
    readonly HashSet<string> _pendingRoutes = [with(StringComparer.Ordinal)];
    bool _disposed;

    internal LeaseInventoryObserver(LeaseClient client, string pattern, LeaseObserveOptions options, TimeProvider time)
    {
        _client = client;
        _pattern = pattern;
        _options = options;
        _time = time;
        _lifetime = _lifetimeCts.Token;
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
            _reconciliationLoopTask = Task.Run(() => ReconciliationLoopAsync(_lifetime), CancellationToken.None);
        }
        catch
        {
            await DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    async ValueTask HandleReconnectAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _disposed) || _lifetime.IsCancellationRequested)
        {
            return;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _lifetime);
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
        catch (OperationCanceledException) when (Volatile.Read(ref _disposed))
        {
            // Disposed concurrently with a reconnect-triggered bootstrap; DisposeAsync
            // already tears down the subscription and background consumer loop itself.
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested || _lifetime.IsCancellationRequested)
        {
            // The reconnect attempt or observer lifetime ended; there is no live
            // recovery target for a background retry.
        }
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            // The restored wire subscription may still be healthy, but a failed LIST
            // leaves the view knowingly stale. Hand recovery to the bounded retry loop;
            // replacing the subscription also closes any ambiguity about the restored
            // registration after a transport failure.
            RequestSubscriptionRecovery();
        }
    }

    async Task BootstrapAsync(CancellationToken ct, bool isReconnect = false)
    {
        await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // A disposed observer's bootstrap must abort cleanly instead of proceeding to
            // mutate state that DisposeAsync is about to (or already did) tear down.
            if (Volatile.Read(ref _disposed))
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
                var consumerLoopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetime);
                _consumerLoopCts = consumerLoopCts;
                _consumerLoopTask = Task.Run(() => ConsumeNotificationsAsync(subscription, consumerLoopCts.Token), CancellationToken.None);
            }

            // 3-5. LIST to completion and install only after a complete pass
            // observed no buffered invalidation. Retry a bounded number of
            // immediate passes, then publish the latest complete snapshot;
            // pending routes and periodic reconciliation converge afterward.
            var convergencePass = 0;
            while (true)
            {
                convergencePass++;
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
                    if (Volatile.Read(ref _disposed))
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

                    if (_bootstrapDirty && convergencePass < MaxImmediateConvergencePasses)
                    {
                        _bootstrapDirty = false;
                        continue;
                    }

                    _bootstrapDirty = false;
                    // Every route buffered before this converged LIST pass is represented by the
                    // snapshot being installed. Leaving those routes pending lets the channel
                    // consumer replay the same notification after readiness flips and issue a
                    // redundant relist against an already-current view.
                    _pendingRoutes.Clear();
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

    void InvalidateNotification(LeaseChangeEvent change)
    {
        lock (_gate)
        {
            if (!_isReady)
            {
                _bootstrapDirty = true;
                _pendingRoutes.Add(change.Route);
            }
            else
            {
                _pendingRoutes.Add(change.Route);
            }
        }
    }

    async Task ConsumeNotificationsAsync(LeaseSubscription subscription, CancellationToken ct)
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
                catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
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
        catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
        {
            // Handler overflow/backpressure removes or invalidates the wire
            // registration. Periodic LIST-only reconciliation cannot repair
            // that gap, so immediately replace the subscription and rebuild
            // the view. The recovery loop owns retry/backoff for transient
            // SUBSCRIBE or LIST failures.
            RequestSubscriptionRecovery();
            return;
        }

        await StopAfterSubscriptionCompletionAsync().ConfigureAwait(false);
    }

    async Task StopAfterSubscriptionCompletionAsync()
    {
        Interlocked.Exchange(ref _reconnectRegistration, null)?.Dispose();
        lock (_gate)
        {
            _isReady = false;
        }

        try
        {
            await _lifetimeCts.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // DisposeAsync won the race.
        }

        _updates.Writer.TryComplete();
    }

    void RequestSubscriptionRecovery()
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) || _lifetime.IsCancellationRequested)
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

    async Task RecoverSubscriptionLoopAsync()
    {
        var attempt = 0;
        while (!Volatile.Read(ref _disposed))
        {
            lock (_gate)
            {
                _subscriptionRecoveryRequested = false;
            }

            try
            {
                await BootstrapAsync(_lifetime).ConfigureAwait(false);
                attempt = 0;
            }
            catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
            {
                return;
            }
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
                attempt++;
                await Task.Delay(NextSubscriptionRecoveryDelay(attempt), _time, _lifetime).ConfigureAwait(false);
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

    static TimeSpan NextSubscriptionRecoveryDelay(int attempt)
    {
        const double baseMilliseconds = 50;
        const double maximumMilliseconds = 2_000;
        const int maximumDoublings = 6;
        const double jitterRatio = 0.2;
        var exponent = Math.Min(Math.Max(attempt - 1, 0), maximumDoublings);
        var capped = TimeSpan.FromMilliseconds(Math.Min(maximumMilliseconds, baseMilliseconds * Math.Pow(2, exponent)));
        var jittered = Jitter.Apply(capped, jitterRatio);
        return jittered < TimeSpan.FromMilliseconds(baseMilliseconds) ? TimeSpan.FromMilliseconds(baseMilliseconds) : jittered;
    }

    async Task HandleSteadyStateNotificationAsync(CancellationToken ct)
    {
        await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
        var updates = new List<LeaseInventoryUpdate>();
        try
        {
            var affectedRoutes = new HashSet<string>(StringComparer.Ordinal);
            var convergencePass = 0;
            while (true)
            {
                convergencePass++;
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

                    if (_pendingRoutes.Count > 0 && convergencePass < MaxImmediateConvergencePasses)
                    {
                        continue;
                    }

                    affectedRoutes.UnionWith(_pendingRoutes);
                    _pendingRoutes.Clear();
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

    async Task ReconciliationLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var delay = NextReconciliationDelay(_options);
            try
            {
                await Task.Delay(delay, _time, ct).ConfigureAwait(false);
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
            catch (Exception error) when (ExceptionPolicy.IsRecoverable(error))
            {
                // Best effort: try again on the next interval.
            }
        }
    }

    static TimeSpan NextReconciliationDelay(LeaseObserveOptions options) =>
        Jitter.Apply(options.ReconciliationInterval, options.ReconciliationJitterRatio);

    async Task<Dictionary<string, LeaseListItem>> ListAllAsync(CancellationToken ct)
    {
        const int maxPages = 10_000;
        using var listDeadline = new CancellationTokenSource(_options.ListTimeout, _time);
        using var listScope = CancellationTokenSource.CreateLinkedTokenSource(ct, listDeadline.Token);
        var listToken = listScope.Token;
        var result = new Dictionary<string, LeaseListItem>(StringComparer.Ordinal);
        LeaseListCursor? cursor = null;
        var pageCount = 0;
        try
        {
            do
            {
                if (++pageCount > maxPages)
                {
                    throw new LeaseException($"LIST exceeded the {maxPages}-page safety limit", "LIST_PAGE_LIMIT_EXCEEDED");
                }

                var page = await _client.ListAsync(_pattern, cursor, limit: null, listToken).ConfigureAwait(false);
                foreach (var item in page.Items)
                {
                    result[item.Route] = item;
                }

                if (page.NextCursor is not null && page.NextCursor == cursor)
                {
                    throw new LeaseException("LIST returned a non-progressing continuation cursor", "LIST_NON_PROGRESSING_CURSOR");
                }

                cursor = page.NextCursor;
            } while (cursor is not null);
        }
        catch (OperationCanceledException error) when (!ct.IsCancellationRequested && listDeadline.IsCancellationRequested)
        {
            throw new LeaseException("Inventory LIST pass timed out", "LIST_TIMEOUT", error);
        }

        return result;
    }

    async Task StopConsumerLoopAsync()
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
        if (Interlocked.Exchange(ref _disposed, true))
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
