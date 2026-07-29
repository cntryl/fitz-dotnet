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
internal sealed class MultiplexerHotPathBenchmarks : IDisposable
{
    private Multiplexer _mux = null!;

    [GlobalSetup]
    public void Setup()
    {
        _mux = new Multiplexer();
        _mux.SetConnected();
    }

    [GlobalCleanup]
    public void Dispose()
    {
        _mux.Dispose();
    }

    [Benchmark]
    public async Task<byte[]> RequestDispatchRoundTrip()
    {
        var task = _mux.RequestAsync(
            450,
            [0x1, 0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5)
        );

        _mux.Dispatch(450, [0x9, 0x8, 0x7]);
        return await task.ConfigureAwait(false);
    }

    [Benchmark]
    public async Task CancellationThenNextDispatch()
    {
        using var cts = new CancellationTokenSource();

        var first = _mux.RequestAsync(
            451,
            [0x1],
            static async (_, token) =>
            {
                await Task.Delay(10, token).ConfigureAwait(false);
            },
            TimeSpan.FromSeconds(5),
            cancellationToken: cts.Token
        );

        var second = _mux.RequestAsync(
            451,
            [0x2],
            static (_, _) => Task.CompletedTask,
            TimeSpan.FromSeconds(5)
        );

        await cts.CancelAsync().ConfigureAwait(false);
        try
        {
            await first.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        _mux.Dispatch(451, [0xA]);
        _ = await second.ConfigureAwait(false);
    }
}
