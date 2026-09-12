namespace Cntryl.Fitz.Observability;

/// <summary>
/// Simple timer for measuring latency of operations.
/// Returns results in microseconds for easy comparison against perf targets.
/// </summary>
sealed class PerfTimer : IDisposable
{
    readonly long _startTicks;
    readonly LatencyHistogram? _histogram;
    long _elapsedTicks;
    bool _disposed;

    /// <summary>Starts timing.</summary>
    /// <param name="histogram">Optional histogram that receives the elapsed time on disposal.</param>
    public PerfTimer(LatencyHistogram? histogram = null)
    {
        _histogram = histogram;
        _startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Get elapsed time in microseconds since construction.
    /// </summary>
    public long ElapsedMicroseconds
    {
        get
        {
            var elapsed = System.Diagnostics.Stopwatch.GetTimestamp() - _startTicks;
            return (long)(elapsed * 1_000_000.0 / System.Diagnostics.Stopwatch.Frequency);
        }
    }

    /// <summary>Stops timing and records the result. Safe to call more than once.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        _elapsedTicks = System.Diagnostics.Stopwatch.GetTimestamp() - _startTicks;
        var microseconds = (long)(_elapsedTicks * 1_000_000.0 / System.Diagnostics.Stopwatch.Frequency);

        _histogram?.Record(microseconds);
    }

    /// <summary>
    /// Create timer, execute action, measure elapsed time.
    /// </summary>
    public static long Measure(Action action, LatencyHistogram? histogram = null)
    {
        ArgumentNullException.ThrowIfNull(action);

        using var timer = new PerfTimer(histogram);
        action();
        return timer.ElapsedMicroseconds;
    }

    /// <summary>
    /// Create timer, execute async action, measure elapsed time.
    /// </summary>
    public static async Task<long> MeasureAsync(Func<Task> action, LatencyHistogram? histogram = null)
    {
        ArgumentNullException.ThrowIfNull(action);

        using var timer = new PerfTimer(histogram);
        await action().ConfigureAwait(false);
        return timer.ElapsedMicroseconds;
    }
}
