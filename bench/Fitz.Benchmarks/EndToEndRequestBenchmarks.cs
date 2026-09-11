using BenchmarkDotNet.Attributes;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Real client request path over an in-memory transport: encode -> request gate -> multiplexer
/// lane -> send -> receive loop -> frame parse -> dispatch -> completion.
/// No network cost is included, so every nanosecond here is client-side work.
/// </summary>
[SimpleJob]
[MemoryDiagnoser]
[ThreadingDiagnoser]
[PlainExporter]
public class EndToEndRequestBenchmarks : IAsyncDisposable
{
    static readonly ushort[] LaneTypes =
    [
        MessageTypes.KvGet,
        MessageTypes.KvPut,
        MessageTypes.QueueEnqueue,
        MessageTypes.LeaseAcquire,
        MessageTypes.NoticePublish,
        MessageTypes.ScheduleCreate,
        MessageTypes.StreamAppend,
        MessageTypes.RpcRequest,
    ];

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
            AuthSettleDelay: TimeSpan.Zero,
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

    public ValueTask DisposeAsync() => _connection.DisposeAsync();

    /// <summary>Sequential request latency. Target from PERF_GUIDELINES: &lt;10 us.</summary>
    [Benchmark(Baseline = true)]
    public async Task SequentialRequests()
    {
        for (var i = 0; i < Concurrency; i++)
        {
            await _connection.RequestAsync(MessageTypes.KvGet, _payload).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// N concurrent requests on ONE message type. The multiplexer keeps a per-message-type
    /// SemaphoreSlim lane, so these serialize regardless of available cores.
    /// </summary>
    [Benchmark]
    public async Task ConcurrentSameMessageType()
    {
        var tasks = new Task[Concurrency];
        for (var i = 0; i < Concurrency; i++)
        {
            tasks[i] = _connection.RequestAsync(MessageTypes.KvGet, _payload).AsTask();
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    /// <summary>
    /// The same N concurrent requests spread across 8 message types, i.e. 8 independent lanes.
    /// The delta against ConcurrentSameMessageType is the cost of lane serialization.
    /// </summary>
    [Benchmark]
    public async Task ConcurrentAcrossMessageTypes()
    {
        var tasks = new Task[Concurrency];
        for (var i = 0; i < Concurrency; i++)
        {
            tasks[i] = _connection.RequestAsync(LaneTypes[i % LaneTypes.Length], _payload).AsTask();
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }
}
