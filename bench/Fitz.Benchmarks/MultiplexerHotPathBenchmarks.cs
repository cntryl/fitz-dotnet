using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Cntryl.Fitz.Connection;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Real multiplexer hot-path benchmarks (request enqueue + dispatch + completion).
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[ThreadingDiagnoser]
[PlainExporter]
public class MultiplexerHotPathBenchmarks : IDisposable
{
    Multiplexer _mux = null!;
    ulong _nextCorrelationId;

    [GlobalSetup]
    public void Setup()
    {
        _mux = new Multiplexer();
        _mux.SetConnected();
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

    [Benchmark]
    public async Task<byte[]> RequestDispatchRoundTrip()
    {
        var task = _mux.RequestAsync(
            450,
            [0x1, 0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            ++_nextCorrelationId
        );

        _mux.DispatchCorrelated(_nextCorrelationId, new byte[] { 0x9, 0x8, 0x7 });
        return await task.ConfigureAwait(false);
    }

    /// <summary>
    /// Cancels one in-flight request (leaving a correlation tombstone) and completes the next.
    /// </summary>
    [Benchmark]
    public async Task CancellationThenNextDispatch()
    {
        using var cts = new CancellationTokenSource();
        var first = _mux.RequestAsync(
            451,
            [0x1],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            ++_nextCorrelationId,
            cts.Token
        );

        var second = _mux.RequestAsync(
            451,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5),
            ++_nextCorrelationId
        );

        await cts.CancelAsync().ConfigureAwait(false);
        try
        {
            await first.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        _mux.DispatchCorrelated(_nextCorrelationId, new byte[] { 0xA });
        _ = await second.ConfigureAwait(false);
    }
}
