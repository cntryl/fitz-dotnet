namespace Cntryl.Fitz.Observability;

/// <summary>
/// Captures latency metrics in microseconds.
/// Used for validating perf targets during integration tests.
/// </summary>
public sealed class LatencyHistogram
{
    private readonly List<long> _samples = new();
    private readonly object _lock = new();

    /// <summary>
    /// Record a single latency sample in microseconds.
    /// </summary>
    public void Record(long microseconds)
    {
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
        lock (_lock)
        {
            if (_samples.Count == 0) return 0;
            var sorted = _samples.OrderBy(x => x).ToList();
            var index = (int)(sorted.Count * percentile) - 1;
            return sorted[Math.Max(0, index)];
        }
    }

    public long P50 => GetPercentile(0.50);
    public long P95 => GetPercentile(0.95);
    public long P99 => GetPercentile(0.99);
    public long P999 => GetPercentile(0.999);

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

    public void Reset()
    {
        lock (_lock)
        {
            _samples.Clear();
        }
    }

    public override string ToString()
    {
        return $"LatencyHistogram(count={Count}, p50={P50}μs, p99={P99}μs, p999={P999}μs, max={Max}μs)";
    }
}
