using BenchmarkDotNet.Attributes;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Real client request path over an in-memory transport: encode -> request gate -> multiplexer
/// -> send -> receive loop -> frame decode -> correlated dispatch -> completion.
/// No network cost is included, so every nanosecond here is client-side work.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[ThreadingDiagnoser]
[PlainExporter]
public class EndToEndRequestBenchmarks : IAsyncDisposable
{
    FitzConnection _connection = null!;
    byte[] _payload = null!;

    [Params(1, 8, 64)]
    public int Concurrency { get; set; }

    /// <summary>Simulated network round-trip. 0 isolates client cost; 1 ms is a LAN hop.</summary>
    [Params(0, 1)]
    public int RoundTripMs { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _payload = new byte[64];
        var config = new ClientConfig(
            new Uri("ws://loopback/ws"),
            Retry: new RetryOptions(Enabled: false),
            Reconnect: new ReconnectOptions(Enabled: false),
            MaxInFlightRequests: 1024,
            MaxRequestQueueSize: 8192);

        _connection = new FitzConnection(
            config,
            () => new LoopbackTransport(roundTripLatency: TimeSpan.FromMilliseconds(RoundTripMs)));
        await _connection.ConnectAsync().ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup() => await DisposeAsync().ConfigureAwait(false);

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    /// <summary>Sequential request latency for the in-memory request path.</summary>
    [Benchmark(Baseline = true)]
    public async Task SequentialRequests()
    {
        for (var i = 0; i < Concurrency; i++)
        {
            await _connection.RequestAsync(MessageTypes.KvGet, _payload).ConfigureAwait(false);
        }
    }

    /// <summary>N concurrent requests, each matched to its caller by correlation identifier.</summary>
    [Benchmark]
    public async Task ConcurrentRequests()
    {
        var tasks = new Task[Concurrency];
        for (var i = 0; i < Concurrency; i++)
        {
            tasks[i] = _connection.RequestAsync(MessageTypes.KvGet, _payload).AsTask();
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }
}
