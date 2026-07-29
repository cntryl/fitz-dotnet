using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Cntryl.Fitz.Runtime;

internal sealed class AsyncHandlerDispatcher
{
    private readonly object _gate = new();
    private readonly Queue<Func<CancellationToken, ValueTask>> _queue = new();
    private readonly HashSet<Task> _activeTasks = new();
    private readonly Action<Exception> _onError;
    private readonly Action<int, int>? _onMetricsChanged;
    private readonly Action<int, int>? _onSaturated;
    private readonly int _maxConcurrency;
    private readonly int _queueCapacity;
    private readonly TimeSpan _timeout;
    private int _activeCount;
    private bool _closed;

    internal AsyncHandlerDispatcher(
        int? maxConcurrency,
        TimeSpan timeout,
        int queueCapacity,
        Action<Exception> onError,
        Action<int, int>? onMetricsChanged = null,
        Action<int, int>? onSaturated = null)
    {
        _maxConcurrency = maxConcurrency is null || maxConcurrency <= 0 ? int.MaxValue : maxConcurrency.Value;
        _timeout = timeout;
        _queueCapacity = queueCapacity < 0 ? 0 : queueCapacity;
        _onError = onError;
        _onMetricsChanged = onMetricsChanged;
        _onSaturated = onSaturated;
    }

    internal bool TryDispatch(Func<CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        lock (_gate)
        {
            if (_closed)
            {
                return false;
            }

            if (_activeCount < _maxConcurrency)
            {
                StartUnsafe(handler);
                return true;
            }

            if (_queue.Count >= _queueCapacity)
            {
                _onSaturated?.Invoke(_activeCount, _queue.Count);
                _onMetricsChanged?.Invoke(_activeCount, _queue.Count);
                return false;
            }

            _queue.Enqueue(handler);
            _onMetricsChanged?.Invoke(_activeCount, _queue.Count);
            return true;
        }
    }

    internal void Close()
    {
        lock (_gate)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            _queue.Clear();
            _onMetricsChanged?.Invoke(_activeCount, _queue.Count);
        }
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

    private void StartUnsafe(Func<CancellationToken, ValueTask> handler)
    {
        _activeCount++;
        _onMetricsChanged?.Invoke(_activeCount, _queue.Count);
        var task = RunAsync(handler);
        _activeTasks.Add(task);
        _ = task.ContinueWith(static (completed, state) => ((AsyncHandlerDispatcher)state!).OnTaskCompleted(completed), this, TaskScheduler.Default);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "The dispatcher isolates arbitrary user callback failures and reports them through the configured error sink.")]
    private async Task RunAsync(Func<CancellationToken, ValueTask> handler)
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

    private void OnTaskCompleted(Task completed)
    {
        lock (_gate)
        {
            _activeTasks.Remove(completed);
            if (_activeCount > 0)
            {
                _activeCount--;
            }

            while (!_closed && _activeCount < _maxConcurrency && _queue.Count > 0)
            {
                var next = _queue.Dequeue();
                if (next is not null)
                {
                    StartUnsafe(next);
                    break;
                }
            }

            _onMetricsChanged?.Invoke(_activeCount, _queue.Count);
        }
    }
}
