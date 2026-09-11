using System.Buffers;
using BenchmarkDotNet.Attributes;
using Cntryl.Fitz.Protocol;
using Cntryl.Fitz.Transport;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Per-component costs on the request and receive hot paths, isolated so the end-to-end
/// figures in <see cref="EndToEndRequestBenchmarks"/> can be attributed.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[PlainExporter]
public class HotPathComponentBenchmarks
{
    byte[] _payload = null!;
    byte[] _twoFrames = null!;
    FrameParser _parser = null!;
    string _route = null!;

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[64];
        _route = "kv://realm/app/users";
        var frame = FrameCodec.Encode(MessageTypes.KvGet, _payload);
        _twoFrames = [.. frame, .. frame];
        _parser = new FrameParser();
    }

    /// <summary>
    /// The full encode a request pays: build the domain payload into the writer's pooled buffer,
    /// then encode a frame around it. Production passes <c>WrittenMemory</c>, so this is the real
    /// shape rather than the extra <c>Build()</c> copy.
    /// </summary>
    [Benchmark(Description = "Encode request payload + frame")]
    public byte[] EncodeRequestFrame()
    {
        using var writer = new BinaryBufferWriter();
        writer.WriteU64(42);
        writer.WriteString(_route);
        writer.WriteU32((uint)_payload.Length);
        writer.WriteBytes(_payload);
        return FrameCodec.Encode(MessageTypes.KvGet, writer.WrittenSpan);
    }

    /// <summary>What the receive loop calls: borrowed payloads, no per-frame allocation.</summary>
    [Benchmark(Description = "Parse: Append + TryReadFrame (receive-loop path)")]
    public int ParseBorrowed()
    {
        _parser.Append(_twoFrames);
        var count = 0;
        while (_parser.TryReadFrame(out _))
        {
            count++;
        }

        return count;
    }

    /// <summary>The owned-payload public API, for the ownership cost <c>BUF-8</c> documents.</summary>
    [Benchmark(Description = "Parse: ParseFrames (owned payloads)")]
    public int ParseCopying() => _parser.ParseFrames(_twoFrames).Count;

    /// <summary>
    /// A received frame's full lifetime: rent sized for the largest message we might see, expose a
    /// small frame, then clear and return. Guards the written-region clearing policy in BUF-5.
    /// </summary>
    [Benchmark(Description = "PooledFrame lifetime (16KB rent, 67B frame)")]
    public int PooledFrameRoundTrip()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
        using var frame = PooledFrame.FromRentedBuffer(buffer, 67);
        return frame.Length;
    }

    /// <summary>
    /// The linked token source and timer every transport send builds to apply its timeout.
    /// </summary>
    [Benchmark(Description = "Per-send linked CTS + CancelAfter")]
    public bool SendTimeoutPlumbing()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.IsCancellationRequested;
    }
}
