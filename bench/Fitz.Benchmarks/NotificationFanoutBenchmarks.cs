using BenchmarkDotNet.Attributes;
using Cntryl.Fitz.Connection;
using Cntryl.Fitz.Protocol;

namespace Cntryl.Fitz.Benchmarks;

/// <summary>
/// Notification delivery: receive loop -> Multiplexer.Dispatch (global lock, handler-array copy,
/// payload copy) -> bounded channel(1024) -> single-reader pump task -> handler.
/// </summary>
/// <remarks>
/// Do not wait for every notification to be delivered here. The pump queue is bounded at 1024 and
/// written with TryWrite, so a burst larger than the queue discards the overflow instead of
/// applying backpressure; waiting on a delivery count deadlocks.
/// </remarks>
[SimpleJob]
[MemoryDiagnoser]
[ThreadingDiagnoser]
[PlainExporter]
public class NotificationFanoutBenchmarks : IDisposable
{
    Multiplexer _mux = null!;
    IDisposable[] _registrations = null!;
    byte[] _payload = null!;
    int _dispatchErrors;

    [Params(1, 8, 64)]
    public int Subscribers { get; set; }

    /// <summary>Notifications discarded because the pump queue was full, over the whole run.</summary>
    public int DispatchErrors => _dispatchErrors;

    [GlobalSetup]
    public void Setup()
    {
        _mux = new Multiplexer(onDispatchError: _ => Interlocked.Increment(ref _dispatchErrors));
        _mux.SetConnected();
        _payload = new byte[128];
        _registrations = new IDisposable[Subscribers];
        for (var i = 0; i < Subscribers; i++)
        {
            _registrations[i] = _mux.RegisterNotificationHandler(MessageTypes.NoticeNotify, static _ => { });
        }
    }

    [GlobalCleanup]
    public void Dispose()
    {
        foreach (var registration in _registrations)
        {
            registration.Dispose();
        }

        _mux.Dispose();
    }

    /// <summary>
    /// Cost the receive loop pays to hand one notification frame to N subscribers: global lock,
    /// handler-array copy, payload copy, and N channel writes.
    /// </summary>
    [Benchmark(Description = "Receive-loop cost of one notification frame, N subscribers")]
    public void DispatchOneNotification() => _mux.Dispatch(MessageTypes.NoticeNotify, _payload);
}
