namespace Cntryl.Fitz.Observability;

/// <summary>
/// Perf summary report for integration tests.
/// Aggregates latency, throughput, and allocation data.
/// </summary>
sealed class PerfSummary
{
    /// <summary>Name of the measured scenario.</summary>
    public string TestName { get; set; } = string.Empty;
    /// <summary>Wall-clock duration of the run, in milliseconds.</summary>
    public long ElapsedMilliseconds { get; set; }
    /// <summary>Median latency, in microseconds.</summary>
    public long P50Microseconds { get; set; }
    /// <summary>99th percentile latency, in microseconds.</summary>
    public long P99Microseconds { get; set; }
    /// <summary>99.9th percentile latency, in microseconds.</summary>
    public long P999Microseconds { get; set; }
    /// <summary>Largest latency, in microseconds.</summary>
    public long MaxMicroseconds { get; set; }
    /// <summary>Operations completed during the run.</summary>
    public long OperationCount { get; set; }
    /// <summary>Throughput achieved during the run.</summary>
    public double OperationsPerSecond { get; set; }
    /// <summary>Outcome against the supplied targets: <c>PASS</c>, <c>WARN</c>, or <c>FAIL</c>.</summary>
    public string Status { get; set; } = "PASS"; // PASS, WARN, FAIL

    /// <summary>Formats the summary for diagnostics.</summary>
    /// <returns>A human-readable summary.</returns>
    public override string ToString()
    {
        return $"""
            {TestName}:
              Elapsed: {ElapsedMilliseconds}ms
              Throughput: {OperationsPerSecond:F2} ops/sec
              Latency: p50={P50Microseconds}μs, p99={P99Microseconds}μs, max={MaxMicroseconds}μs
              Status: {Status}
            """;
    }

    /// <summary>Builds a summary from a latency histogram and a throughput meter.</summary>
    /// <param name="testName">Name of the measured scenario.</param>
    /// <param name="histogram">Recorded latencies.</param>
    /// <param name="meter">Recorded throughput.</param>
    /// <param name="p99Target">Optional 99th percentile target, in microseconds.</param>
    /// <param name="throughtputTarget">Optional throughput target, in operations per second.</param>
    /// <returns>The populated summary, with <see cref="Status"/> set against the targets.</returns>
    public static PerfSummary FromHistogram(
        string testName,
        LatencyHistogram histogram,
        ThroughputMeter meter,
        long? p99Target = null,
        long? throughtputTarget = null)
    {
        ArgumentNullException.ThrowIfNull(histogram);
        ArgumentNullException.ThrowIfNull(meter);

        var summary = new PerfSummary
        {
            TestName = testName,
            ElapsedMilliseconds = (long)meter.Elapsed.TotalMilliseconds,
            P50Microseconds = histogram.P50,
            P99Microseconds = histogram.P99,
            P999Microseconds = histogram.P999,
            MaxMicroseconds = histogram.Max,
            OperationCount = histogram.Count,
            OperationsPerSecond = meter.OperationsPerSecond,
        };

        // Determine status based on targets
        var warnings = new List<string>();

        if (p99Target.HasValue && histogram.P99 > p99Target)
            warnings.Add($"p99 {histogram.P99}μs > target {p99Target}μs");

        if (throughtputTarget.HasValue && meter.OperationsPerSecond < throughtputTarget)
            warnings.Add($"throughput {meter.OperationsPerSecond:F2} ops/sec < target {throughtputTarget} ops/sec");

        summary.Status = warnings.Count > 0 ? "WARN" : "PASS";

        return summary;
    }
}
