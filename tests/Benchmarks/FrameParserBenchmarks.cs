using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

[SimpleJob]
[MemoryDiagnoser]
[PlainExporter]
public class FrameParserBenchmarks
{
    private FrameParser _parser = null!;
    private byte[] _singleFrame = null!;
    private byte[] _twoFrames = null!;
    private byte[] _chunkA = null!;
    private byte[] _chunkB = null!;

    [GlobalSetup]
    public void Setup()
    {
        _parser = new FrameParser();

        _singleFrame = FrameCodec.Encode(100, [0x1, 0x2, 0x3, 0x4]);
        var second = FrameCodec.Encode(101, [0x9, 0x8]);

        _twoFrames = new byte[_singleFrame.Length + second.Length];
        _singleFrame.CopyTo(_twoFrames, 0);
        second.CopyTo(_twoFrames, _singleFrame.Length);

        var split = Math.Max(1, _singleFrame.Length / 2);
        _chunkA = _singleFrame.AsSpan(0, split).ToArray();
        _chunkB = _singleFrame.AsSpan(split).ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _parser = new FrameParser();
    }

    [Benchmark]
    public int ParseSingleFrame()
    {
        var frames = _parser.ParseFrames(_singleFrame);
        return frames.Count;
    }

    [Benchmark]
    public int ParseTwoFramesBatch()
    {
        var frames = _parser.ParseFrames(_twoFrames);
        return frames.Count;
    }

    [Benchmark]
    public int ParseSplitFrameAcrossChunks()
    {
        var first = _parser.ParseFrames(_chunkA);
        var second = _parser.ParseFrames(_chunkB);
        return first.Count + second.Count;
    }
}
