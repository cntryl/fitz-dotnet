using System.Diagnostics;

namespace Cntryl.Fitz.Observability;

/// <summary>
/// Measures throughput (operations per second) over a time window.
/// </summary>
public sealed class ThroughputMeter
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private long _operationCount;
    private readonly object _lock = new();

    /// <summary>
    /// Record N operations completed.
    /// </summary>
    public void RecordOperations(int count)
    {
        lock (_lock)
        {
            _operationCount += count;
        }
    }

    /// <summary>
    /// Record one operation completed.
    /// </summary>
    public void RecordOperation()
    {
        lock (_lock)
        {
            _operationCount++;
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

    public TimeSpan Elapsed => _stopwatch.Elapsed;

    public void Reset()
    {
        lock (_lock)
        {
            _stopwatch.Restart();
            _operationCount = 0;
        }
    }

    public override string ToString()
    {
        return $"ThroughputMeter(ops={TotalOperations}, ops/sec={OperationsPerSecond:F2}, elapsed={Elapsed.TotalSeconds:F2}s)";
    }
}
