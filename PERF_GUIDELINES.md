# Fitz .NET Performance-First Design Guidelines

This document establishes mandatory patterns and targets for high-performance async messaging in fitz-dotnet on .NET 10.

## Objectives

- **Hotpath latency:** <10 microseconds for request-response roundtrip
- **Throughput:** 25–50% improvement vs .NET 9 baseline
- **Allocations:** <40% of legacy allocations for streaming workloads
- **Concurrency:** Safe at 5,000+ concurrent RPC streams with <2 μs correlation lookup

## Mandatory Patterns

### 1. ValueTask for Single-Response Hot Paths

Use `ValueTask<T>` instead of `Task<T>` for operations that commonly complete synchronously (single response expected).

**Scope:**
- KV `.GetAsync()`, `.BeginAsync()`
- Lease `.QueryAsync()`, `.AcquireAsync()`
- RPC unary `.RequestAsync()` (if ever implemented without streaming)
- Schedule `.CreateAsync()`, `.CancelAsync()`

**Rationale:** .NET 10 escape analysis eliminates allocation when result is already available; avoids Task heap allocation for fast paths.

**Example:**
```csharp
public ValueTask<Memory<byte>> GetAsync(
    string key,
    CancellationToken ct = default)
{
    // Fast path: cached or synchronous local result
    if (TryGetCached(key, out var result))
        return new ValueTask<Memory<byte>>(result);

    // Slow path: allocate Task-based ValueTask for async wait
    return new ValueTask<Memory<byte>>(GetAsyncCore(key, ct));
}

private async Task<Memory<byte>> GetAsyncCore(string key, CancellationToken ct)
{
    // Standard async logic
}
```

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
- Schedule `.ListAsync()` — pagination
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

| Phase | Per-Request (encode→dispatch) | Per-Subscription | Per-Streaming Record |
|-------|------------------------------|-------------------|----------------------|
| Baseline (.NET 9) | ~500 bytes | ~200 bytes (overhead) | ~100 bytes (List<T> grow) |
| Target (.NET 10) | <150 bytes | <50 bytes (Channel ref only) | <10 bytes (Channel struct) |
| Savings | 70% reduction | 75% reduction | 90% reduction |

## Benchmarking Integration

Every phase includes explicit perf validation:

1. **Encode/decode microbenchmarks** (BenchmarkDotNet)
   - Target: <100 ns encode, <200 ns decode
   - Run: `dotnet run --project tests/Benchmarks/Benchmarks.csproj`

2. **Correlation lookup stress test**
   - Target: <2 μs @ 5K concurrent RPC streams
   - Measured via BenchmarkDotNet with concurrent loop

3. **Integration perf validation**
   - Each integration test records latency distribution
   - Target: <50 μs KV roundtrip, <20 μs RPC frame dispatch
   - Aggregated in test output

4. **Allocation profiling**
   - Run with dotTrace or PerfView
   - Target: <40% of legacy allocation rate for streaming workloads

## CI Integration

- Perf benchmarks run in release mode only
- Baselines autogenerated on first run; regression detection on subsequent runs
- BenchmarkDotNet results committed to bench_results/ for historical tracking
- Allocations [profiled periodically](./PERF_BENCHMARKS.md) via dotTrace

## Code Review Checklist

Before merging async/critical-path code, verify:

- [ ] No `new byte[]` allocations in hot paths (use ArrayPool)
- [ ] ConfigureAwait(false) on all awaits
- [ ] Single try/finally structure (no complex control flow)
- [ ] ValueTask used for single-response paths
- [ ] IAsyncEnumerable used (not List<T> return)
- [ ] Callback closures stack-allocatable (simple capture)
- [ ] Partitioned vs global correlation storage justified
- [ ] Benchmark measurement added/updated
- [ ] Target latency validated locally (release mode)

## References

- [.NET 10 Performance Features](https://github.com/dotnet/runtime/wiki/Releases)
- [System.Threading.Channels](https://learn.microsoft.com/en-us/dotnet/api/system.threading.channels)
- [ValueTask<T>](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.valuetask-1)
- [Span<T>](https://learn.microsoft.com/en-us/dotnet/api/system.span-1)
- [BenchmarkDotNet](https://benchmarkdotnet.org/)

---

**Last Updated:** 2026-03-17  
**Target Framework:** .NET 10.0  
**Expected Perf Gain:** 25–50% throughput, 15–35% latency reduction, 3–10× allocation savings
