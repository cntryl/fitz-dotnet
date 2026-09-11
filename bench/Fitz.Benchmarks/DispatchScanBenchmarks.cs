using BenchmarkDotNet.Attributes;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Cost of one <see cref="Multiplexer.Dispatch(ushort, byte[])"/> as the pending-request list for a
/// message type grows. This runs on the single receive-loop thread while holding the multiplexer's
/// global lock, so it bounds how fast the client can route frames.
/// </summary>
/// <remarks>
/// Only correlated requests (those supplying a response matcher) can queue more than one deep — an
/// uncorrelated request holds its message-type lane until its response arrives. No production call
/// site supplies a matcher today, so this measures the headroom a future correlated design would
/// inherit rather than a cost the client pays now.
/// </remarks>
[SimpleJob]
[MemoryDiagnoser]
[PlainExporter]
public class DispatchScanBenchmarks : IDisposable
{
    Multiplexer _mux = null!;
    byte[] _frame = null!;
    byte[] _matchingResponse = null!;

    [Params(1, 16, 128, 1024)]
    public int PendingDepth { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _mux = new Multiplexer();
        _mux.SetConnected();
        _frame = new byte[64];
        _matchingResponse = [0xEE, 0x01];

        // Park PendingDepth - 1 correlated requests that never match. They stay queued for the
        // whole run, so each measured dispatch walks past all of them before finding its target
        // and no per-iteration setup is needed to reach a steady state.
        for (var i = 0; i < PendingDepth - 1; i++)
        {
            _ = _mux.RequestAsync(
                MessageTypes.RpcRequest,
                _frame,
                static (_, _) => Task.CompletedTask,
                Timeout.InfiniteTimeSpan,
                responseMatcher: static _ => false);
        }
    }

    [GlobalCleanup]
    public void Dispose() => _mux.Dispose();

    /// <summary>
    /// Registers one matching request behind the parked ones, then dispatches the response that
    /// completes it. The scan walks the whole list, invoking every matcher along the way.
    /// </summary>
    [Benchmark(Description = "Dispatch one response past N pending correlated requests")]
    public async Task<byte[]> DispatchPastPending()
    {
        var request = _mux.RequestAsync(
            MessageTypes.RpcRequest,
            _frame,
            static (_, _) => Task.CompletedTask,
            Timeout.InfiniteTimeSpan,
            responseMatcher: static payload => payload.Span[0] == 0xEE);

        _mux.Dispatch(MessageTypes.RpcRequest, _matchingResponse);
        return await request.ConfigureAwait(false);
    }
}
