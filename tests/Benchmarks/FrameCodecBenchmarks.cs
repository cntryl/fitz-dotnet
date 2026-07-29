using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Frame codec benchmarks: encoding and decoding hot-path performance.
/// </summary>
[SimpleJob(warmupCount: 3, iterationCount: 5)]
[MemoryDiagnoser]
[PlainExporter]
internal sealed class FrameCodecBenchmarks
{
    private byte[]? _payload64;
    private byte[]? _payload256;
    private byte[]? _payload1024;
    private byte[]? _encoded64;
    private byte[]? _encoded256;
    private byte[]? _encoded1024;
    private byte[]? _encodedLarge;
    private byte[]? _payload4096;

    [GlobalSetup]
    public void Setup()
    {
        _payload64 = new byte[64];
        _payload256 = new byte[256];
        _payload1024 = new byte[1024];
        _payload4096 = new byte[4096];

        for (int i = 0; i < _payload4096.Length; i++)
            _payload4096[i] = (byte)(i % 256);

        _payload64.AsSpan().Fill(0xAA);
        _payload256.AsSpan().Fill(0xBB);
        _payload1024.AsSpan().Fill(0xCC);

        _encoded64 = FrameCodec.Encode(100, _payload64);
        _encoded256 = FrameCodec.Encode(200, _payload256);
        _encoded1024 = FrameCodec.Encode(300, _payload1024);
        _encodedLarge = FrameCodec.Encode(400, _payload4096);
    }

    [Benchmark]
    public byte[] EncodeSmallMessage()
    {
        return FrameCodec.Encode(100, _payload64!);
    }

    [Benchmark]
    public byte[] EncodeMediumMessage()
    {
        return FrameCodec.Encode(200, _payload256!);
    }

    [Benchmark]
    public byte[] EncodeLargeMessage()
    {
        return FrameCodec.Encode(300, _payload1024!);
    }

    [Benchmark]
    public Frame DecodeSmallMessage()
    {
        return FrameCodec.Decode(_encoded64!);
    }

    [Benchmark]
    public Frame DecodeMediumMessage()
    {
        return FrameCodec.Decode(_encoded256!);
    }

    [Benchmark]
    public Frame DecodeLargeMessage()
    {
        return FrameCodec.Decode(_encoded1024!);
    }

    [Benchmark]
    public Frame DecodeXLargeMessage()
    {
        return FrameCodec.Decode(_encodedLarge!);
    }

    [Benchmark]
    public byte[] EncodeExtendedMessageType()
    {
        return FrameCodec.Encode(500, _payload256!);
    }
}
