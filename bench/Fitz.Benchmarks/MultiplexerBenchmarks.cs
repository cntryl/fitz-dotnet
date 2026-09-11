using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Dictionary-operation baselines used to contextualize the real multiplexer benchmarks.
/// These results are not measurements of the Fitz multiplexer.
/// </summary>
[SimpleJob]
[ThreadingDiagnoser]
[MemoryDiagnoser]
[PlainExporter]
public class CorrelationDictionaryBaselineBenchmarks
{
    ConcurrentDictionary<ushort, object> _correlations = null!;
    object[] _handlers = null!;

    [Params(10, 100, 1000, 5000)]
    public int ConcurrencyLevel { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _correlations = new ConcurrentDictionary<ushort, object>();
        _handlers = new object[ConcurrencyLevel];

        // Pre-populate with handlers
        for (var i = 0; i < ConcurrencyLevel; i++)
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
    /// Baseline dictionary lookup plus object consumption; no dispatch or channel work is included.
    /// </summary>
    [Benchmark]
    public int LookupAndConsumeValue()
    {
        var messageType = (ushort)(System.DateTime.UtcNow.Ticks % ConcurrencyLevel);
        if (_correlations.TryGetValue(messageType, out var handler))
        {
            return handler.GetHashCode();
        }
        return 0;
    }
}
