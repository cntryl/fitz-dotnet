namespace Cntryl.Fitz.Observability;

/// <summary>
/// Perf summary report for integration tests.
/// Aggregates latency, throughput, and allocation data.
/// </summary>
public sealed class PerfSummary
{
    public string TestName { get; set; } = string.Empty;
    public long ElapsedMilliseconds { get; set; }
    public long P50Microseconds { get; set; }
    public long P99Microseconds { get; set; }
    public long P999Microseconds { get; set; }
    public long MaxMicroseconds { get; set; }
    public long OperationCount { get; set; }
    public double OperationsPerSecond { get; set; }
    public string Status { get; set; } = "PASS"; // PASS, WARN, FAIL

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
