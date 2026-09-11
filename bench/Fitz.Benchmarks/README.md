# Fitz .NET Performance Benchmarks

BenchmarkDotNet microbenchmarks for the Fitz .NET client hot paths.

Every benchmark here measures Fitz code. Benchmarks that timed runtime primitives rather than
this client (an `ArrayPool` vs `new byte[]` comparison, a bare `ConcurrentDictionary` standing in
for the multiplexer) were removed — they reported healthy numbers for code the client never runs,
and those numbers were being read as multiplexer results.

## Running

Release mode is mandatory:

```bash
dotnet run --project bench/Fitz.Benchmarks/Fitz.Benchmarks.csproj -c Release -- --filter '*'
```

Run one class:

```bash
dotnet run --project bench/Fitz.Benchmarks/Fitz.Benchmarks.csproj -c Release -- \
  --filter '*EndToEndRequestBenchmarks*'
```

The defaults in this repo's committed reports were once `IterationCount=1, WarmupCount=1`, which is
not a measurement. Pass at least `--warmupCount 3 --iterationCount 10`, and prefer more for
anything reported outside a local investigation.

## Classes

### EndToEndRequestBenchmarks

The real `FitzConnection` request path over an in-memory transport: encode, request gate,
multiplexer lane, send, receive loop, frame parse, dispatch, completion.

`RoundTripMs` adds simulated network latency. At `0` every nanosecond is client-side work; at `1`
the results show what the per-message-type request lane costs, because `ConcurrentSameMessageType`
matches `SequentialRequests` exactly while `ConcurrentAcrossMessageTypes` is ~7.8× faster. That
lane is `MUX-1` in `docs/sharp-edges-evidence-ledger.md` — protocol-deferred, not a local defect.

### HotPathComponentBenchmarks

Per-component costs used to attribute the end-to-end totals: request encode, the borrowed
receive-loop parse against the owned-payload public API, pooled frame lifetime, and the per-send
cancellation plumbing.

### MultiplexerHotPathBenchmarks

Request enqueue, dispatch and completion against the real `Multiplexer`, including the
cancellation-then-next-dispatch path.

### DispatchScanBenchmarks

How one dispatch scales with pending-request depth for a message type. Only correlated requests
can queue deeper than one, and no production call site supplies a response matcher today, so this
measures headroom for a future correlated design rather than a cost paid now.

### NotificationFanoutBenchmarks

What the receive loop pays to hand one notification frame to N subscribers. Cost is flat in
subscriber count: the fan-out array is cached per message type and the pump, not the receive loop,
invokes the handlers.

Do not wait on a delivery count in a benchmark here. The pump queue is bounded and written with
`TryWrite`, so a burst larger than the queue discards the overflow instead of applying
backpressure, and waiting for every notification deadlocks.

### FrameCodecBenchmarks

TLV encode and decode across payload sizes, including the extended message-type header.

### DomainHotPathBenchmarks

Domain clients (KV, queue, lease, notice, schedule) against stub responders, covering request
serialization and response parsing per domain.

## Targets

From [PERF_GUIDELINES.md](../../PERF_GUIDELINES.md):

| Operation | Target | Covered by |
|-----------|--------|------------|
| Frame encode | <100 ns | `FrameCodecBenchmarks` |
| Frame decode | <200 ns | `FrameCodecBenchmarks`, `HotPathComponentBenchmarks` |
| RPC dispatch per frame | <5 µs | `DispatchScanBenchmarks` |
| Request-response round trip | <10 µs | `EndToEndRequestBenchmarks` (`RoundTripMs=0`) |
| Allocation per request | <150 B | `EndToEndRequestBenchmarks` |

The concurrency targets in that document (5,000 concurrent RPC streams, <2 µs correlation lookup
at that load) are not reachable while `MUX-1` stands, because a request holds its message-type
lane until its response arrives.

## Adding a benchmark

Measure code in `src/`. If a benchmark would pass with the client's own logic deleted, it belongs
in a runtime experiment, not here. State the target it defends in a docstring, and add it to the
class list above.
