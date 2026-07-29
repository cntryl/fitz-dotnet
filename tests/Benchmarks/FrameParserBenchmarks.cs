using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

[SimpleJob]
[MemoryDiagnoser]
[PlainExporter]
internal sealed class FrameParserBenchmarks
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
        _parser.Append(_singleFrame);
        return _parser.TryReadFrame(out _) ? 1 : 0;
    }

    [Benchmark]
    public int ParseTwoFramesBatch()
    {
        _parser.Append(_twoFrames);

        var count = 0;
        while (_parser.TryReadFrame(out _))
        {
            count++;
        }

        return count;
    }

    [Benchmark]
    public int ParseSplitFrameAcrossChunks()
    {
        _parser.Append(_chunkA);
        var count = 0;

        while (_parser.TryReadFrame(out _))
        {
            count++;
        }

        _parser.Append(_chunkB);
        while (_parser.TryReadFrame(out _))
        {
            count++;
        }

        return count;
    }
}
