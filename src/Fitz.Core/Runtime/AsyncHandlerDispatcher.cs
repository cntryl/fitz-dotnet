using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Cntryl.Fitz.Runtime;

sealed class AsyncHandlerDispatcher
{
    readonly object _gate = new();
    readonly Queue<DispatchWorkItem> _queue = new();
    readonly HashSet<Task> _activeTasks = [];
    readonly Action<Exception> _onError;
    readonly Action<int, int>? _onMetricsChanged;
    readonly Action<int, int>? _onSaturated;
    readonly int _maxConcurrency;
    readonly int _queueCapacity;
    readonly TimeSpan _timeout;
    int _activeCount;
    bool _closed;

    internal AsyncHandlerDispatcher(
        int? maxConcurrency,
        TimeSpan timeout,
        int queueCapacity,
        Action<Exception> onError,
        Action<int, int>? onMetricsChanged = null,
        Action<int, int>? onSaturated = null)
    {
        _maxConcurrency = maxConcurrency ?? Math.Max(1, Environment.ProcessorCount);
        _timeout = timeout;
        _queueCapacity = queueCapacity < 0 ? 0 : queueCapacity;
        _onError = onError;
        _onMetricsChanged = onMetricsChanged;
        _onSaturated = onSaturated;
    }

    internal bool TryDispatch(
        Func<CancellationToken, ValueTask> handler,
        Action<Exception>? onRejected = null)
    {
        ArgumentNullException.ThrowIfNull(handler);

        var accepted = true;
        var saturated = false;
        int activeCount;
        int queuedCount;
        lock (_gate)
        {
            if (_closed)
            {
                return false;
            }

            if (_activeCount < _maxConcurrency)
            {
                StartUnsafe(new DispatchWorkItem(handler, onRejected));
            }
            else if (_queue.Count >= _queueCapacity)
            {
                accepted = false;
                saturated = true;
            }
            else
            {
                _queue.Enqueue(new DispatchWorkItem(handler, onRejected));
            }

            activeCount = _activeCount;
            queuedCount = _queue.Count;
        }

        ReportMetrics(activeCount, queuedCount, saturated);
        return accepted;
    }

    internal void Close()
    {
        int activeCount;
        DispatchWorkItem[] queued;
        lock (_gate)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            queued = [.. _queue];
            _queue.Clear();
            activeCount = _activeCount;
        }

        var failure = new OperationCanceledException("The async handler dispatcher is closed.");
        foreach (var workItem in queued)
        {
            workItem.Reject(failure);
        }
        ReportMetrics(activeCount, 0, saturated: false);
    }

    internal async Task DrainAsync()
    {
        Task[] tasks;
        lock (_gate)
        {
            tasks = _activeTasks.ToArray();
        }

        if (tasks.Length == 0)
        {
            return;
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    void StartUnsafe(DispatchWorkItem workItem)
    {
        _activeCount++;
        var task = Task.Run(() => RunAsync(workItem.Handler));
        _activeTasks.Add(task);
        _ = task.ContinueWith(static (completed, state) => ((AsyncHandlerDispatcher)state!).OnTaskCompleted(completed), this, TaskScheduler.Default);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "The dispatcher isolates arbitrary user callback failures and reports them through the configured error sink.")]
    async Task RunAsync(Func<CancellationToken, ValueTask> handler)
    {
        using var timeoutCts = _timeout == System.Threading.Timeout.InfiniteTimeSpan
            ? null
            : new CancellationTokenSource(_timeout);
        var token = timeoutCts?.Token ?? CancellationToken.None;

        try
        {
            await handler(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _onError(new TimeoutException($"Async handler timed out after {_timeout.TotalMilliseconds}ms."));
        }
        catch (Exception ex)
        {
            _onError(ex);
        }
    }

    void OnTaskCompleted(Task completed)
    {
        int activeCount;
        int queuedCount;
        lock (_gate)
        {
            _activeTasks.Remove(completed);
            if (_activeCount > 0)
            {
                _activeCount--;
            }

            while (!_closed && _activeCount < _maxConcurrency && _queue.Count > 0)
            {
                StartUnsafe(_queue.Dequeue());
                break;
            }

            activeCount = _activeCount;
            queuedCount = _queue.Count;
        }

        ReportMetrics(activeCount, queuedCount, saturated: false);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Observability callbacks must not corrupt dispatcher state.")]
    void ReportMetrics(int activeCount, int queuedCount, bool saturated)
    {
        try
        {
            if (saturated)
            {
                _onSaturated?.Invoke(activeCount, queuedCount);
            }
            _onMetricsChanged?.Invoke(activeCount, queuedCount);
        }
        catch
        {
        }
    }

    readonly record struct DispatchWorkItem(
        Func<CancellationToken, ValueTask> Handler,
        Action<Exception>? Rejection)
    {
        [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "A rejection callback must not interrupt dispatcher shutdown.")]
        internal void Reject(Exception exception)
        {
            try
            {
                Rejection?.Invoke(exception);
            }
            catch
            {
            }
        }
    }
}
