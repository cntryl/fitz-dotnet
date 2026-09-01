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
    private readonly Channel<LeaseInventoryUpdate> _updates = Channel.CreateUnbounded<LeaseInventoryUpdate>(
        new UnboundedChannelOptions { SingleReader = false, SingleWriter = false });
    private readonly CancellationTokenSource _lifetimeCts = new();

    private IDisposable? _reconnectRegistration;
    private LeaseSubscription? _subscription;
    private CancellationTokenSource? _consumerLoopCts;
    private Task? _consumerLoopTask;
    private Task? _reconciliationLoopTask;
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

    private async ValueTask HandleReconnectAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        lock (_gate)
        {
            _isReady = false;
        }
        await BootstrapAsync(ct).ConfigureAwait(false);
    }

    private async Task BootstrapAsync(CancellationToken ct)
    {
        await _refreshGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            lock (_gate)
            {
                _isReady = false;
                _bootstrapDirty = false;
                _pendingRoutes.Clear();
            }

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

            // 2. Start buffering incoming notifications for this subscription; they are not
            // applied until one LIST pass completes without a concurrent invalidation.
            var consumerLoopCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeCts.Token);
            _consumerLoopCts = consumerLoopCts;
            _consumerLoopTask = Task.Run(() => ConsumeNotificationsAsync(subscription, consumerLoopCts.Token), CancellationToken.None);

            // 3-5. LIST to completion and install only after a complete pass
            // observed no buffered invalidation. Repeating instead of
            // marking ready before the drain closes the acknowledgement /
            // pagination race for an arbitrarily long bootstrap.
            while (true)
            {
                var freshView = await ListAllAsync(ct).ConfigureAwait(false);
                lock (_gate)
                {
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

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Best-effort background loop; errors are corrected by the next reconciliation or notification.")]
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
        }
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

        await StopConsumerLoopAsync().ConfigureAwait(false);

        if (_subscription is not null)
        {
            await _subscription.DisposeAsync().ConfigureAwait(false);
            _subscription = null;
        }

        _updates.Writer.TryComplete();
        _lifetimeCts.Dispose();
        _refreshGate.Dispose();
    }
}
