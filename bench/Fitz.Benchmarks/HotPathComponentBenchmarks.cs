using System.Buffers;
using BenchmarkDotNet.Attributes;
using Cntryl.Fitz.Protocol;

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
    byte[] _frame = null!;
    string _route = null!;

    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[64];
        _route = "kv://realm/app/users";
        _frame = FrameCodec.Encode(MessageTypes.KvGet, _payload, FrameCodec.ResponseFinalFlag, correlationId: 1);
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

    /// <summary>What the receive loop calls for each transport message.</summary>
    [Benchmark(Description = "Decode one frame (receive-loop path)")]
    public int DecodeFrame() => FrameCodec.DecodeStrict(_frame).Payload.Length;

    /// <summary>
    /// A received frame's full lifetime: rent sized for the largest message we might see, expose a
    /// small frame, then clear and return. Guards the written-region clearing policy in BUF-5.
    /// </summary>
    [Benchmark(Description = "PooledFrame lifetime (16KB rent, 67B frame)")]
    public static int PooledFrameRoundTrip()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
        using var frame = PooledFrame.FromRentedBuffer(buffer, 67);
        return frame.Length;
    }

    /// <summary>
    /// The linked token source and timer every transport send builds to apply its timeout.
    /// </summary>
    [Benchmark(Description = "Per-send linked CTS + CancelAfter")]
    public static bool SendTimeoutPlumbing()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        return cts.IsCancellationRequested;
    }
}
