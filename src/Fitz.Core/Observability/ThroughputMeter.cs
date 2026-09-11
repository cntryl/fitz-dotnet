using System.Diagnostics;

namespace Cntryl.Fitz.Observability;

/// <summary>
/// Measures throughput (operations per second) over a time window.
/// </summary>
public sealed class ThroughputMeter
{
    readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    long _operationCount;
    readonly object _lock = new();

    /// <summary>
    /// Record N operations completed.
    /// </summary>
    public void RecordOperations(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        lock (_lock)
        {
            _operationCount = checked(_operationCount + count);
        }
    }

    /// <summary>
    /// Record one operation completed.
    /// </summary>
    public void RecordOperation()
    {
        lock (_lock)
        {
            _operationCount = checked(_operationCount + 1);
        }
    }

    /// <summary>
    /// Get current throughput in operations per second.
    /// </summary>
    public double OperationsPerSecond
    {
        get
        {
            lock (_lock)
            {
                var elapsed = _stopwatch.Elapsed.TotalSeconds;
                return elapsed > 0 ? _operationCount / elapsed : 0;
            }
        }
    }

    /// <summary>
    /// Get total operations recorded.
    /// </summary>
    public long TotalOperations
    {
        get
        {
            lock (_lock)
            {
                return _operationCount;
            }
        }
    }

    /// <summary>Time elapsed since the meter started or was last reset.</summary>
    public TimeSpan Elapsed => _stopwatch.Elapsed;

    /// <summary>Clears the operation count and restarts the clock.</summary>
    public void Reset()
    {
        lock (_lock)
        {
            _stopwatch.Restart();
            _operationCount = 0;
        }
    }

    /// <summary>Formats the counts and rate for diagnostics.</summary>
    /// <returns>A human-readable summary.</returns>
    public override string ToString() => $"ThroughputMeter(ops={TotalOperations}, ops/sec={OperationsPerSecond:F2}, elapsed={Elapsed.TotalSeconds:F2}s)";
}
