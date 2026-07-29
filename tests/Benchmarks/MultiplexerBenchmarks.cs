using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System.Collections.Concurrent;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Benchmarks for request correlation and multiplexing operations.
/// Target: lookup <2 μs @ 5K concurrent, dispatch <5 μs per frame
/// </summary>
[SimpleJob]
[ThreadingDiagnoser]
[MemoryDiagnoser]
[PlainExporter]
internal sealed class MultiplexerBenchmarks
{
    private ConcurrentDictionary<ushort, object> _correlations = null!;
    private object[] _handlers = null!;

    [Params(10, 100, 1000, 5000)]
    public int ConcurrencyLevel { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _correlations = new ConcurrentDictionary<ushort, object>();
        _handlers = new object[ConcurrencyLevel];

        // Pre-populate with handlers
        for (int i = 0; i < ConcurrencyLevel; i++)
        {
            _handlers[i] = new object();
            _correlations.TryAdd((ushort)(i % ushort.MaxValue), _handlers[i]);
        }
    }

    /// <summary>
    /// Benchmark: TryGetValue lookup on correlation dictionary (uncontended)
    /// Target: <200 ns uncontended, <2 μs @ 5K concurrent
    /// </summary>
    [Benchmark]
    public bool CorrelationLookupUncontended()
    {
        var messageType = (ushort)(System.DateTime.UtcNow.Ticks % ConcurrencyLevel);
        return _correlations.TryGetValue(messageType, out _);
    }

    /// <summary>
    /// Benchmark: TryAdd + TryRemove pair (registration/cleanup cycle)
    /// Target: <1 μs per cycle
    /// </summary>
    [Benchmark]
    public bool CorrelationRegisterUnregister()
    {
        var id = (ushort)(System.DateTime.UtcNow.Ticks % ConcurrencyLevel);
        var handler = new object();

        if (_correlations.TryAdd(id, handler))
        {
            _correlations.TryRemove(id, out _);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Benchmark: Dispatch to callback (simulated response handling)
    /// Target: <5 μs with Channel write overhead
    /// </summary>
    [Benchmark]
    public void DispatchResponse()
    {
        var messageType = (ushort)(System.DateTime.UtcNow.Ticks % ConcurrencyLevel);
        if (_correlations.TryGetValue(messageType, out var handler))
        {
            // Placeholder: simulate callback invocation
            _ = handler.GetHashCode();
        }
    }
}
