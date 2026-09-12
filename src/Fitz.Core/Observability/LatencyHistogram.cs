namespace Cntryl.Fitz.Observability;

/// <summary>
/// Captures latency metrics in microseconds.
/// Used for validating perf targets during integration tests.
/// </summary>
sealed class LatencyHistogram
{
    readonly List<long> _samples = [];
    readonly object _lock = new();

    /// <summary>
    /// Record a single latency sample in microseconds.
    /// </summary>
    public void Record(long microseconds)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(microseconds);
        lock (_lock)
        {
            _samples.Add(microseconds);
        }
    }

    /// <summary>
    /// Get percentile (e.g., 0.99 = p99).
    /// </summary>
    public long GetPercentile(double percentile)
    {
        if (double.IsNaN(percentile) || percentile < 0 || percentile > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(percentile), "Percentile must be between 0 and 1 inclusive.");
        }

        lock (_lock)
        {
            if (_samples.Count == 0)
                return 0;
            var sorted = _samples.OrderBy(x => x).ToList();
            var index = (int)(sorted.Count * percentile) - 1;
            return sorted[Math.Max(0, index)];
        }
    }

    /// <summary>Median recorded latency, in microseconds.</summary>
    public long P50 => GetPercentile(0.50);
    /// <summary>95th percentile latency, in microseconds.</summary>
    public long P95 => GetPercentile(0.95);
    /// <summary>99th percentile latency, in microseconds.</summary>
    public long P99 => GetPercentile(0.99);
    /// <summary>99.9th percentile latency, in microseconds.</summary>
    public long P999 => GetPercentile(0.999);

    /// <summary>Arithmetic mean latency, in microseconds.</summary>
    public long Mean
    {
        get
        {
            lock (_lock)
            {
                return _samples.Count > 0 ? (long)_samples.Average() : 0;
            }
        }
    }

    /// <summary>Largest recorded latency, in microseconds.</summary>
    public long Max
    {
        get
        {
            lock (_lock)
            {
                return _samples.Count > 0 ? _samples.Max() : 0;
            }
        }
    }

    /// <summary>Number of samples recorded.</summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _samples.Count;
            }
        }
    }

    /// <summary>Discards every recorded sample.</summary>
    public void Reset()
    {
        lock (_lock)
        {
            _samples.Clear();
        }
    }

    /// <summary>Formats the count and key percentiles for diagnostics.</summary>
    /// <returns>A human-readable summary.</returns>
    public override string ToString() => $"LatencyHistogram(count={Count}, p50={P50}μs, p99={P99}μs, p999={P999}μs, max={Max}μs)";
}
