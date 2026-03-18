using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using System.Buffers;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Benchmarks for frame encoding and decoding operations.
/// Target: encode <100 ns, decode <200 ns
/// </summary>
[SimpleJob(RuntimeMoniker.Net90)]
[MemoryDiagnoser]
[PlainExporter]
[MeanColumn]
[MedianColumn]
[StdDevColumn]
[AllStatisticsColumn]
public class FrameCodecBenchmarks
{
    private static readonly byte[] MessageTypes = [0x01, 0x14, 0x64, 0xc8];
    private static readonly int[] PayloadSizes = [0, 64, 256, 1024];
    
    // Placeholder: will be populated with actual frame codec implementations
    private byte[] _smallPayload = null!;
    private byte[] _largePayload = null!;
    private byte[] _frameBuffer = null!;

    [GlobalSetup]
    public void Setup()
    {
        _smallPayload = new byte[64];
        _largePayload = new byte[1024];
        _frameBuffer = ArrayPool<byte>.Shared.Rent(2048);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        ArrayPool<byte>.Shared.Return(_frameBuffer);
    }

    /// <summary>
    /// Benchmark: encode small payload (64 bytes)
    /// Target: <100 ns
    /// </summary>
    [Benchmark]
    public int EncodeSmallPayload()
    {
        // Placeholder: will use actual FrameCodec.EncodeFrame()
        // Return actual length for verification
        return _smallPayload.Length + 8; // Approximate frame overhead
    }

    /// <summary>
    /// Benchmark: encode large payload (1024 bytes)
    /// Target: <500 ns
    /// </summary>
    [Benchmark]
    public int EncodeLargePayload()
    {
        // Placeholder: will use actual FrameCodec.EncodeFrame()
        return _largePayload.Length + 8;
    }

    /// <summary>
    /// Benchmark: decode frame header and extract payload slice
    /// Target: <200 ns (no allocation)
    /// </summary>
    [Benchmark]
    public int DecodeFrame()
    {
        // Placeholder: will use actual FrameCodec.DecodeFrame()
        // Should return payload length
        return _smallPayload.Length;
    }
}
