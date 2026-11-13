using BenchmarkDotNet.Attributes;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Cost of one <see cref="Multiplexer.DispatchCorrelated"/> as the number of in-flight requests
/// grows. This runs on the single receive-loop thread while holding the multiplexer's global lock,
/// so it bounds how fast the client can route frames. Lookup is by correlation identifier, so the
/// cost should stay flat as depth increases.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[PlainExporter]
public class DispatchScanBenchmarks : IDisposable
{
    Multiplexer _mux = null!;
    byte[] _frame = null!;
    byte[] _response = null!;
    ulong _nextCorrelationId;

    [Params(1, 16, 128, 1024)]
    public int PendingDepth { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _mux = new Multiplexer();
        _mux.SetConnected();
        _frame = new byte[64];
        _response = [0xEE, 0x01];

        // Park PendingDepth - 1 requests that are never answered. They stay in flight for the
        // whole run, so no per-iteration setup is needed to reach a steady state.
        for (var i = 0; i < PendingDepth - 1; i++)
        {
            _ = _mux.RequestAsync(
                MessageTypes.RpcRequest,
                _frame,
                static (_, _) => Task.CompletedTask,
                Timeout.InfiniteTimeSpan,
                ++_nextCorrelationId);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _mux.Dispose();
        }
    }

    /// <summary>
    /// Registers one request alongside the parked ones, then dispatches the response that
    /// completes it.
    /// </summary>
    [Benchmark(Description = "Dispatch one response with N requests in flight")]
    public async Task<byte[]> DispatchPastPending()
    {
        var request = _mux.RequestAsync(
            MessageTypes.RpcRequest,
            _frame,
            static (_, _) => Task.CompletedTask,
            Timeout.InfiniteTimeSpan,
            ++_nextCorrelationId);

        _mux.DispatchCorrelated(_nextCorrelationId, _response);
        return await request.ConfigureAwait(false);
    }
}
