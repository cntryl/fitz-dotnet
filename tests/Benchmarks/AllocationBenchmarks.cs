using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System.Buffers;
using System.Security.Cryptography;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Benchmarks focused on allocation patterns and GC pressure.
/// Target: <40% allocations vs eager List<T> buffering
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[PlainExporter]
internal sealed class AllocationBenchmarks
{
    private byte[] _buffer = null!;
    private const int PayloadSize = 1024;

    [GlobalSetup]
    public void Setup()
    {
        _buffer = new byte[PayloadSize];
        RandomNumberGenerator.Fill(_buffer);
    }

    /// <summary>
    /// Benchmark: ArrayPool.Rent() allocation pattern (preferred)
    /// Target: <100 bytes allocation per request
    /// </summary>
    [Benchmark]
    public int ArrayPoolAllocation()
    {
        var rented = ArrayPool<byte>.Shared.Rent(PayloadSize);
        try
        {
            System.Array.Copy(_buffer, rented, PayloadSize);
            return rented.Length;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
	/// Benchmark: eager new byte[] allocation baseline
    /// Target: show regression vs ArrayPool
    /// </summary>
    [Benchmark(Baseline = true)]
    public int NewByteArrayAllocation()
    {
        var buffer = new byte[PayloadSize];
        System.Array.Copy(_buffer, buffer, PayloadSize);
        return buffer.Length;
    }

    /// <summary>
    /// Benchmark: stack-allocated Span<byte> (zero-copy)
    /// Target: show improvement vs heap allocation
    /// </summary>
    [Benchmark]
    public int StackAllocSpan()
    {
        Span<byte> buffer = stackalloc byte[PayloadSize];
        _buffer.AsSpan().CopyTo(buffer);
        return buffer.Length;
    }
}
