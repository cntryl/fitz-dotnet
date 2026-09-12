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
public class FrameCodecBenchmarks
{
    byte[]? _payload64;
    byte[]? _payload256;
    byte[]? _payload1024;
    byte[]? _encoded64;
    byte[]? _encoded256;
    byte[]? _encoded1024;
    byte[]? _encodedLarge;
    byte[]? _payload4096;

    [GlobalSetup]
    public void Setup()
    {
        _payload64 = new byte[64];
        _payload256 = new byte[256];
        _payload1024 = new byte[1024];
        _payload4096 = new byte[4096];

        for (var i = 0; i < _payload4096.Length; i++)
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
    public byte[] EncodeSmallMessage() => FrameCodec.Encode(100, _payload64!);

    [Benchmark]
    public byte[] EncodeMediumMessage() => FrameCodec.Encode(200, _payload256!);

    [Benchmark]
    public byte[] EncodeLargeMessage() => FrameCodec.Encode(300, _payload1024!);

    // BenchmarkDotNet requires public benchmark methods, and Frame is internal, so these
    // return the decoded payload instead. The decode work measured is unchanged: reading
    // one field off the returned struct neither boxes nor allows dead-code elimination.
    [Benchmark]
    public ReadOnlyMemory<byte> DecodeSmallMessage() => FrameCodec.DecodeStrict(_encoded64!).Payload;

    [Benchmark]
    public ReadOnlyMemory<byte> DecodeMediumMessage() => FrameCodec.DecodeStrict(_encoded256!).Payload;

    [Benchmark]
    public ReadOnlyMemory<byte> DecodeLargeMessage() => FrameCodec.DecodeStrict(_encoded1024!).Payload;

    [Benchmark]
    public ReadOnlyMemory<byte> DecodeXLargeMessage() => FrameCodec.DecodeStrict(_encodedLarge!).Payload;

    [Benchmark]
    public byte[] EncodeExtendedMessageType() => FrameCodec.Encode(500, _payload256!);
}
