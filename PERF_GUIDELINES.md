# Fitz .NET Performance-First Design Guidelines

This document establishes mandatory patterns and targets for high-performance async messaging in fitz-dotnet on .NET 10.

## Objectives

- **Hotpath latency:** <10 microseconds for request-response roundtrip; <5 microseconds
  for multiplexer dispatch
- **Allocations:** bounded and pooled on streaming paths — no per-frame `new byte[]`
- **Concurrency:** safe at 5,000+ concurrent RPC streams with <2 μs correlation lookup

Targets are validated by the BenchmarkDotNet suite in `bench/Fitz.Benchmarks`, which is
the only source of performance numbers for this repo. Do not quote a figure here that a
benchmark in that project does not produce.

## Mandatory Patterns

### 1. Task for Public Operations, ValueTask for Internal Hot Paths

**Public one-shot operations return `Task`/`Task<T>`.** Every one of them reaches the
broker, so none completes synchronously and `ValueTask` would buy nothing while costing
consumers the awkward single-await rule. `ValueTask` is used only where it pays:
disposal, callback and provider contracts, and internal request plumbing that genuinely
can complete synchronously.

**Scope:** internal transport and dispatch paths — `ITransport.ReceiveAsync`, the
internal `request` delegates threaded through every domain client, and
`AsyncHandlerDispatch`.

**Rationale:** `ValueTask` is an optimization for methods that usually complete without
suspending. A network round-trip never does. Using it on the public surface would
transfer a correctness hazard — no double-await, no blocking `.Result` — to consumers in
exchange for nothing.

**Example:** the internal seam is `ValueTask`, the public method is `Task`.

```csharp
// Internal: can complete synchronously from a pooled buffer.
internal Func<ushort, ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _request;

// Public: always a round trip.
public async Task<IKvTransaction> BeginAsync(
    string route,
    KvDurability durability,
    KvMode mode = KvMode.ReadWrite,
    CancellationToken ct = default)
```

This reverses earlier guidance that mandated `ValueTask` on public one-shot operations.
The public surface is `Task`; see the preview migration note in the README.

### 2. Channels<T> for High-Concurrency Queues (No Locks)

Replace `Queue<T> + Mutex` with `System.Threading.Channels.Channel<T>` for subscriptions, message queues, and any scenario with >100 concurrent producers/consumers.

**Scope:**
- RPC correlation ID → response frame routing (5K+ concurrent)
- Notice subscriber notification delivery (fanout)
- Queue enqueue/reserve pipelines
- Stream commit notifications
- Schedule execution events

**Rationale:** .NET 10 Channels uses lock-free linked-list internally; supports backpressure; handles cancellation efficiently without GC pressure.

**Example:**
```csharp
private sealed class RpcResponseHandler
{
    private readonly Channel<RpcResponseFrame> _channel = Channel.CreateUnbounded<RpcResponseFrame>(
        new UnboundedChannelOptions { SingleReader = false, SingleWriter = false });

    public async IAsyncEnumerable<RpcResponseFrame> StreamAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var frame in _channel.Reader.ReadAllAsync(ct))
        {
            yield return frame;
        }
    }

    public async ValueTask WriteAsync(RpcResponseFrame frame, CancellationToken ct)
    {
        await _channel.Writer.WriteAsync(frame, ct);
    }
}
```

### 3. IAsyncEnumerable<T> Without Buffering

Stream responses via `IAsyncEnumerable<T>` with `[EnumeratorCancellation]` support. Never buffer results into `List<T>`.

**Scope:**
- Stream `.ReadAsync()` — record iteration
- RPC `.CallStreamingAsync()` — response frame iteration
- Queue `.ReserveAsync()` — item iteration (when paged)
- Schedule `.ListPageAsync()` — pagination
- Notice notifications (implicit via subscription callback)

**Rationale:** .NET 10 bounds-check elimination and array devirtualization make iteration zero-copy; Channel backpressure prevents memory exhaustion; enables true streaming without intermediate buffering.

**Example:**
```csharp
public async IAsyncEnumerable<StreamRecord> ReadAsync(
    long offset,
    long limit,
    [EnumeratorCancellation] CancellationToken ct = default)
{
    using var channel = Channel.CreateUnbounded<StreamRecord>();
    
    // Register frame handler (callback pattern)
    var handler = RegisterStreamHandler(offset, (frame) => {
        channel.Writer.TryWrite(frame.Record);
    });

    try
    {
        await SendReadRequestAsync(offset, limit, ct);
        
        await foreach (var record in channel.Reader.ReadAllAsync(ct))
        {
            yield return record;
        }
    }
    finally
    {
        UnregisterStreamHandler(handler);
        channel.Writer.Complete();
    }
}
```

### 4. ArrayPool for Frame Encoding (Sub-100ns Target)

Use `ArrayPool<byte>.Shared.Rent()` for frame buffer allocation instead of `new byte[]`. Measure encode latency; target <100 nanoseconds.

**Scope:**
- Frame TLV encoding in FrameCodec
- WebSocket message buffering
- Any temporary byte buffers >256 bytes

**Rationale:** .NET 10 GC pressure reduction; cache locality; eliminates Gen2 collections for request storms.

**Example:**
```csharp
public static void EncodeFrame(ushort messageType, ReadOnlySpan<byte> payload, out byte[] frameBytes)
{
    var totalLength = EstimateFrameSize(messageType, payload.Length);
    frameBytes = ArrayPool<byte>.Shared.Rent(totalLength);

    var offset = 0;
    EncodeMessageType(frameBytes, ref offset, messageType);
    EncodeLength(frameBytes, ref offset, payload.Length);
    payload.CopyTo(frameBytes.AsSpan(offset));
    
    // Caller responsible for returning to pool in finally block
}

// Usage:
EncodeFrame(MessageTypes.KvSet, payload, out var frameBytes);
try
{
    await transport.SendAsync(frameBytes.AsMemory(0, actualLength), ct);
}
finally
{
    ArrayPool<byte>.Shared.Return(frameBytes);
}
```

### 5. Span<T>-Based Protocols (Zero-Copy)

Use `ReadOnlySpan<byte>` for frame parsing and `Span<T>` for encoding. Avoid intermediate byte[] copies.

**Scope:**
- Frame header parsing
- Message type/length extraction
- Payload slicing (no copy)

**Rationale:** .NET 10 bounds-check elimination; JIT inlining of Span operations; stack allocation of enumerators.

**Example:**
```csharp
public static (ushort MessageType, ReadOnlySpan<byte> Payload) DecodeFrame(ReadOnlySpan<byte> frame)
{
    var offset = 0;
    var messageType = DecodeMessageType(frame, ref offset); // No allocation, direct span slicing
    var length = DecodeLength(frame, ref offset);
    var payload = frame.Slice(offset, length);
    
    return (messageType, payload);  // Span is stack-allocated ref type
}
```

### 6. ConfigureAwait(false) Pervasive

Apply `.ConfigureAwait(false)` to every `await` expression in async methods (especially in receive loops, correlation dispatch, and timeout handlers).

**Rationale:** Eliminates SynchronizationContext capture on UI-free server paths; prevents ThreadPool starvation under high concurrency; improves throughput under load.

**Example:**
```csharp
public async Task ReceiveLoopAsync(CancellationToken ct)
{
    await foreach (var frame in transport.ReceiveFramesAsync(ct).ConfigureAwait(false))
    {
        await multiplexer.DispatchAsync(frame, ct).ConfigureAwait(false);
    }
}
```

### 7. Callback Closures Without Heap Allocation (.NET 10 Escape Analysis)

Use inline delegate/lambda callbacks for correlation handlers and subscription callbacks. .NET 10 escape analysis stack-allocates closures when captured variables are short-lived.

**Scope:**
- Correlation ID → response handler callback registration
- Timeout cancellation callbacks
- Subscription callbacks (publish-subscribe)

**Rationale:** .NET 10 feature; previously these would heap-allocate; now often stack-allocated, eliminating GC pressure.

**Example:**
```csharp
public async ValueTask<Memory<byte>> RequestAsync(
    ushort messageType,
    ReadOnlyMemory<byte> frameData,
    CancellationToken ct = default)
{
    var tcs = new TaskCompletionSource<Memory<byte>>();
    
    // Register callback (closure over 'tcs')—.NET 10 escape analysis stack-allocates this
    RegisterCorrelationHandler(messageType, (response) =>
    {
        tcs.TrySetResult(response);
    });

    try
    {
        await SendAsync(frameData, ct).ConfigureAwait(false);
        return await tcs.Task.ConfigureAwait(false);
    }
    finally
    {
        UnregisterCorrelationHandler(messageType);
    }
}
```

### 8. Try/Finally for Cleanup (Inlining-Friendly)

Structure exception handling as simple try/finally (not complex control flow). .NET 10 JIT can inline methods with try/finally blocks.

**Rationale:** Enables JIT inlining of cleanup paths; improves performance of error scenarios.

**Example:**
```csharp
// ✓ Good: simple try/finally, inlineable
public async ValueTask<bool> TryDoWorkAsync(CancellationToken ct)
{
    var resource = AcquireResource();
    try
    {
        await DoWorkAsync(resource, ct).ConfigureAwait(false);
        return true;
    }
    catch (OperationCanceledException)
    {
        return false;  // Simple handling
    }
    finally
    {
        resource.Dispose();
    }
}

// ✗ Avoid: complex dispatch, not inlineable
public async ValueTask<bool> BadTryDoWorkAsync(CancellationToken ct)
{
    try
    {
        switch (state)
        {
            case 1: await Case1Async(ct); break;
            case 2: await Case2Async(ct); break;
            default: throw new InvalidOperationException();
        }
        return true;
    }
    catch (TimeoutException ex) when (ex.InnerException is SomeSpecialException)
    {
        // Complex exception dispatch
        return RetryAsync(ct);
    }
    finally { }  // Never reached in some branches
}
```

### 9. Partitioned Correlation Storage for Lock-Free Concurrency

For high-concurrency scenarios (5K+ RPC streams), partition correlation storage to avoid lock contention on a global dictionary.

**Scope:**
- RPC correlation ID → response handler mapping (partitioned)
- Multiplexer dispatch table (optional, if per-message-type FIFO is insufficient)

**Rationale:** Each partition stays cache-hot; reduces lock wait time; enables safe concurrent access without global bottleneck.

**Example:**
```csharp
public sealed class PartitionedRpcCorrelations
{
    private const int PartitionCount = Environment.ProcessorCount * 2;
    private readonly ConcurrentDictionary<ReadOnlyMemory<byte>, Func<RpcResponseFrame, ValueTask>>[] _partitions;

    public PartitionedRpcCorrelations()
    {
        _partitions = new ConcurrentDictionary<ReadOnlyMemory<byte>, Func<RpcResponseFrame, ValueTask>>[PartitionCount];
        for (int i = 0; i < PartitionCount; i++)
            _partitions[i] = new ConcurrentDictionary<ReadOnlyMemory<byte>, Func<RpcResponseFrame, ValueTask>>();
    }

    private int GetPartition(ReadOnlySpan<byte> correlationId)
    {
        unchecked
        {
            uint hash = 0;
            for (int i = 0; i < Math.Min(4, correlationId.Length); i++)
                hash = hash * 31 + correlationId[i];
            return (int)(hash % PartitionCount);
        }
    }

    public void Register(Span<byte> correlationId, Func<RpcResponseFrame, ValueTask> handler)
    {
        var partition = GetPartition(correlationId);
        _partitions[partition][new ReadOnlyMemory<byte>(correlationId.ToArray())] = handler;
    }
}
```

### 10. Zero Runtime Reflection

The shipped packages resolve nothing through runtime metadata. Reflection costs
startup time, defeats trimming, and is the usual reason a library is only
"AOT-compatible" rather than AOT-clean.

**Scope:** all of `Fitz.Core`, `Fitz.Abstractions`, and `Fitz.DependencyInjection`.

| Instead of | Use |
|---|---|
| `Enum.IsDefined(value)` | `value is not A and not B` |
| `$"{enumValue}"` / `enumValue.ToString()` | a `switch` mapping each member to `nameof` |
| `obj.GetType().Name` for diagnostics | a constant-returning member on the abstraction (`ITransport.TransportName`) |
| `services.AddSingleton<T>()` | `services.AddSingleton(static sp => new T(...))` |
| reflection-based `JsonSerializer` | source-generated `JsonSerializerContext` |

**Rationale:** the first two keep enum name tables alive and allocate through a
metadata lookup on paths that should be a compare. The DI overloads have the
container select and invoke constructors reflectively on every startup. None of
these buy anything the explicit form does not.

**Also watch generic flow.** Passing an unannotated `T` into a BCL generic that
annotates its type parameter — `Lazy<T>` and `ActivatorUtilities` are the ones hit
here — propagates a reflection contract even when no reflection occurs. Prefer
deleting the contract over annotating it with `DynamicallyAccessedMembers`.

**Verification:** the in-build analyzers are necessary but not sufficient; they
reason one method at a time. The authoritative check is the whole-program ILC
analysis in the `package` CI job, which roots every shipped assembly. See
[docs/aot-and-reflection.md](docs/aot-and-reflection.md).

## Latency Targets (Validation Checkpoints)

| Operation | Target | Rationale |
|-----------|--------|-----------|
| **Frame encode** | <100 ns | ArrayPool + Span + bounds-check elim. |
| **Frame decode** | <200 ns | Span-based slicing, no allocation |
| **Correlation lookup (uncontended)** | <200 ns | Dictionary/ConcurrentDict, single access |
| **Correlation lookup @ 5K concurrent** | <2 μs | Partitioning, minimal lock time |
| **RPC dispatch per frame** | <5 μs | Channel write + callback, no allocation |
| **IAsyncEnumerable yield** | <1 μs | Bounds-check elim. + stack enumerator |
| **Full request-response roundtrip** | <10 μs | Sub-millisecond target (excludes network) |
| **Connection handshake (WebSocket → auth)** | <100 ms | Includes TLS handshake + JWT validation |

## Allocation Budget

There is no measured pre-.NET-10 baseline for this client, so this section records what
has actually been measured and what is still aspirational. Do not add a row without a
benchmark behind it.

**Measured — allocation.** `MultiplexerHotPathBenchmarks.RequestDispatchRoundTrip` allocates
**1.02 KB** per dispatch round trip (2026-09-12, Apple Silicon, .NET 10.0.11, default job).
This reproduces an earlier run that recorded 1.03 KB, so the figure is stable across machines.
It is not yet a per-request budget worth holding anyone to.

**Not established — dispatch latency.** The same run measured **85.9 μs mean, with a 48.4 μs
standard deviation and a 19% confidence margin over 99 iterations**. An earlier run of this
benchmark was recorded at 2.098 μs; that figure carries no record of its hardware or job
configuration and did not reproduce here. Both numbers cannot be right, and the variance means
this benchmark is not currently a reliable latency instrument on the hardware it was re-run on.

Treat the 5 μs dispatch target in the table above as a **design goal, not a measured result**.
Nothing in this repository currently demonstrates that the multiplexer meets it. Establishing
that needs a benchmark whose variance is small enough to support the claim, on hardware whose
configuration is recorded alongside the number.

**Aspirational, not yet validated.** Per-subscription steady-state overhead limited to
the channel reference, and per-streaming-record delivery that does not allocate beyond
the pooled frame. Both need a dedicated benchmark before either becomes a target anyone
is held to.

The binding rules are the pooling and bounds requirements in the patterns above —
no per-frame `new byte[]`, pooled buffers returned on every path, bounded queues — not a
byte count no measurement supports.

## Benchmarking Integration

Every phase includes explicit perf validation:

1. **Encode/decode microbenchmarks** (BenchmarkDotNet)
   - Target: <100 ns encode, <200 ns decode
   - Run: `dotnet run --project bench/Fitz.Benchmarks/Fitz.Benchmarks.csproj`

2. **Correlation lookup stress test**
   - Target: <2 μs @ 5K concurrent RPC streams
   - Measured via BenchmarkDotNet with concurrent loop

3. **Integration perf validation**
   - Each integration test records latency distribution
   - Target: <50 μs KV roundtrip, <20 μs RPC frame dispatch
   - Aggregated in test output

4. **Allocation profiling**
   - Run with dotTrace or PerfView
	- Target: <40% of the eager-allocation baseline for streaming workloads

## CI Integration

- Perf benchmarks run in Release mode only, via `dotnet run -c Release --project bench/Fitz.Benchmarks`
- BenchmarkDotNet writes to the gitignored `BenchmarkDotNet.Artifacts/`; results are not
  committed, so quote a number only alongside the run that produced it
- Benchmarks are not currently part of an automated CI gate. Performance claims belong in
  a pull request description or [the evidence ledger](docs/sharp-edges-evidence-ledger.md),
  with the measurement attached

## Code Review Checklist

Before merging async/critical-path code, verify:

- [ ] No `new byte[]` allocations in hot paths (use ArrayPool)
- [ ] ConfigureAwait(false) on all awaits
- [ ] Single try/finally structure (no complex control flow)
- [ ] `Task` on public operations; `ValueTask` only on internal paths that can complete synchronously
- [ ] IAsyncEnumerable used (not List<T> return)
- [ ] Callback closures stack-allocatable (simple capture)
- [ ] Partitioned vs global correlation storage justified
- [ ] No runtime reflection: no `Enum.IsDefined`, enum `ToString`, `GetType()`, or DI type activation
- [ ] Benchmark measurement added/updated
- [ ] Target latency validated locally (release mode)

## References

- [.NET 10 Performance Features](https://github.com/dotnet/runtime/wiki/Releases)
- [System.Threading.Channels](https://learn.microsoft.com/en-us/dotnet/api/system.threading.channels)
- [ValueTask<T>](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.valuetask-1)
- [Span<T>](https://learn.microsoft.com/en-us/dotnet/api/system.span-1)
- [BenchmarkDotNet](https://benchmarkdotnet.org/)

---

**Last Updated:** 2026-09-11
**Target Framework:** .NET 10.0  
**Status:** the patterns above are binding; the numeric targets are validated only where
a benchmark in `bench/Fitz.Benchmarks` produces the figure.
