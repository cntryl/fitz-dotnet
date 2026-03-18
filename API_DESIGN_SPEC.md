# Fitz .NET Optimal API Surface Design Specification

**Version**: 1.0  
**Date**: March 18, 2026  
**Status**: Phase 1 Design (Pre-Implementation)  
**Alignment**: Go/Python/TypeScript reference SDKs + .NET idioms + PERF_GUIDELINES.md

---

## Executive Summary

This specification defines the complete API surface for fitz-dotnet that achieves:
1. **Feature Parity**: All 7 domains (KV, Queue, RPC, Lease, Notice, Stream, Schedule) with 100% operation coverage
2. **API Consistency**: Method signatures aligned with reference SDKs (Go/Py/Ts)
3. **.NET Idioms**: Idiomatic C# patterns (async/await, IAsyncEnumerable, sealed records)
4. **Performance Targets**: <10μs hotpath latency (ValueTask on critical paths, minimal allocations)
5. **Developer Experience**: Intuitive API surface with minimal boilerplate

---

## 1. Client Entry Point & Connection Lifecycle

### Current State ✅ (No Change Needed)
```csharp
// Entry point
IClient client = new FitzClient(config);
await client.ConnectAsync(cancellationToken);

// Domain access
IKvClient kv = client.Kv();
IQueueClient queue = client.Queue();
// ... (Lease, Stream, Notice, Schedule, Rpc)

// Connection state
bool isConnected = client.IsConnected;

// Cleanup
await client.DisposeAsync();
```

### Design Decisions
- ✅ **Factory methods** over properties (matches Go/Py/Ts)
- ✅ **Lazy initialization** with `??=` null-coalescing (allocates once, reused)
- ✅ **IAsyncDisposable** for resource cleanup during disconnect
- ✅ **ConnectionState enum** (Disconnected, Connecting, Authenticating, Authenticated, Closed)

---

## 2. Configuration & Authentication

### Current State ✅ (Enhancement)
```csharp
// Functional configuration pattern
var config = new ClientConfig(
    Url: "wss://fitz-server:4090",
    Transport: "ws",
    Timeout: TimeSpan.FromSeconds(30),
    AuthSettleDelay: TimeSpan.FromMilliseconds(100),
    TokenProvider: async (ct) => 
    {
        // Async JWT generation
        var token = await GenerateJwtAsync(ct);
        return token;
    },
    TransportFactory: null  // Use default WebSocket resolver
);

await client.ConnectAsync(config, cancellationToken);
```

### Enhancement: Optional Builder Chain
```csharp
// Alternative: Fluent builder (optional, for discoverability)
var config = ClientConfig.Builder()
    .WithUrl("wss://fitz-server:4090")
    .WithTimeout(TimeSpan.FromSeconds(30))
    .WithTokenProvider(GenerateJwtAsync)
    .Build();
```

### Design Decisions
- ✅ Keep **record-based config** as primary (immutable, functional options pattern)
- ⚠️ Add **optional builder** for enhanced discoverability (Builder() factory method)
- ✅ TokenProvider as `Func<CancellationToken, ValueTask<string>>` (supports both sync and async token sources)
- ✅ TransportFactory for testing (allows mocking TCP/WebSocket)

---

## 3. Operation Categories & Performance Tiers

### Tier A: Single-Response Hotpath Operations (Must use ValueTask<T>)
**Target**: <10μs latency, minimal allocations

- **KV**: Get, Insert, Delete, DeleteRange (+ Commit on small transactions)
- **Lease**: Acquire, Query, Extend, Release
- **Notice**: Publish
- **Schedule**: Create, Cancel
- **Queue**: Enqueue
- **RPC**: Request (unary, no streaming)
- **Stream**: Metadata, Append (single record)

```csharp
// Hotpath: Use ValueTask<T>
public ValueTask<KvGetResult> GetAsync(byte[] key, CancellationToken ct = default);
public ValueTask<ILease> AcquireAsync(string route, ulong ttlSecs, CancellationToken ct = default);
public ValueTask<ulong> EnqueueAsync(string route, byte[] body, int? delayMs = null, CancellationToken ct = default);
```

### Tier B: Streaming/Iterator Operations (Use Task<IAsyncEnumerable<T>>)
**Target**: Low memory footprint, backpressure support via async enumeration

- **KV**: Scan (multi-record range query)
- **Stream**: Read (streaming records from offset)
- **RPC**: Call (streaming response frames)

```csharp
// Streaming: Use Task<IAsyncEnumerable<T>>
public Task<IAsyncEnumerable<KvPair>> ScanAsync(string route, KvScanQuery query, CancellationToken ct = default);
public Task<IAsyncEnumerable<StreamRecord>> ReadAsync(string route, ulong startOffset, ulong limit, CancellationToken ct = default);
public Task<IAsyncEnumerable<RpcResponseFrame>> CallAsync(string route, byte[] body, CancellationToken ct = default);
```

### Tier C: Subscription/Event Stream Operations (Use Task<IAsyncEnumerable<T>> or IDisposable)
**Target**: Long-lived connections, event-driven architecture

- **Queue**: Subscribe (availability notifications)
- **Lease**: Subscribe (change notifications)
- **Notice**: Subscribe (message broadcasts)
- **Stream**: Subscribe (commit notifications)
- **Schedule**: Subscribe (execution notifications)
- **RPC**: RegisterWorker (subscription for incoming requests)

```csharp
// Subscriptions: Either async enumerable OR disposable subscription
// Option A: IAsyncEnumerable (consumer-driven pull)
public Task<IAsyncEnumerable<QueueAvailabilityEvent>> SubscribeAsync(string pattern, CancellationToken ct = default);

// Option B: Disposable subscription (producer-driven push via callback)
public Task<IDisposable> SubscribeAsync(string pattern, Func<QueueAvailabilityEvent, ValueTask> handler, CancellationToken ct = default);
```

**Recommendation**: Use **Option A (IAsyncEnumerable)** for new APIs. It's idiomatic in modern .NET and supports consumer backpressure naturally.

---

## 4. Domain API Specifications

### 4.1 Key-Value (KV) Domain

```csharp
public interface IKvClient
{
    /// <summary>Begins a KV transaction with specified mode and durability.</summary>
    ValueTask<IKvTransaction> BeginAsync(
        string route,
        KvMode mode = KvMode.ReadWrite,
        KvDurability durability = KvDurability.Async,
        CancellationToken cancellationToken = default);
}

public interface IKvTransaction
{
    /// <summary>Reads a key within the transaction.</summary>
    ValueTask<KvGetResult> GetAsync(byte[] key, CancellationToken cancellationToken = default);

    /// <summary>Writes a key-value pair (upsert).</summary>
    ValueTask PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Inserts a key-value pair (fails if key exists).</summary>
    ValueTask InsertAsync(byte[] key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Deletes a single key.</summary>
    ValueTask DeleteAsync(byte[] key, CancellationToken cancellationToken = default);

    /// <summary>Deletes keys in range [startKey, endKey).</summary>
    ValueTask DeleteRangeAsync(byte[] startKey, byte[] endKey, CancellationToken cancellationToken = default);

    /// <summary>Scans keys in range, returns async enumerator. Supports backpressure via async enumeration.</summary>
    Task<IAsyncEnumerable<KvPair>> ScanAsync(KvScanQuery query, CancellationToken cancellationToken = default);

    /// <summary>Commits transaction atomically.</summary>
    ValueTask CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>Rolls back transaction (aborts all changes).</summary>
    ValueTask RollbackAsync(CancellationToken cancellationToken = default);
}

// Helper Types
public sealed record KvGetResult(bool Found, byte[]? Value = null);
public sealed record KvPair(byte[] Key, byte[] Value);
public sealed record KvScanQuery(byte[]? StartKey = null, byte[]? EndKey = null, ulong? Limit = null, bool Reverse = false);
public enum KvMode { ReadOnly, ReadWrite }
public enum KvDurability { Async, Sync }
```

---

### 4.2 Queue Domain

```csharp
public interface IQueueClient
{
    /// <summary>Enqueues a message with optional delay.</summary>
    ValueTask<ulong> EnqueueAsync(
        string route,
        byte[] body,
        int? delayMs = null,
        CancellationToken cancellationToken = default);

    /// <summary>Reserves messages from queue (leases with TTL).</summary>
    ValueTask<QueueItem[]> ReserveAsync(
        string route,
        ulong leaseSeconds,
        int batchSize = 1,
        int? waitSeconds = null,
        CancellationToken cancellationToken = default);

    /// <summary>Subscribes to queue availability notifications (async enumerable).</summary>
    Task<IAsyncEnumerable<QueueAvailabilityEvent>> SubscribeAsync(
        string pattern,
        CancellationToken cancellationToken = default);
}

public interface IQueueItem
{
    byte[] Body { get; }
    ulong Id { get; }
    ulong Token { get; }  // Fencing token for ack/extend

    /// <summary>Extends lease duration for this message.</summary>
    ValueTask ExtendAsync(ulong leaseSeconds, CancellationToken cancellationToken = default);

    /// <summary>Marks message as complete (removes from queue).</summary>
    ValueTask CompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>Marks message complete with explicit fencing token.</summary>
    ValueTask CompleteWithTokenAsync(ulong token, CancellationToken cancellationToken = default);
}

// Helper Types
public sealed record QueueItem(string Route, ulong Id, ulong Token, byte[] Body) : IQueueItem { ... }
public sealed record QueueAvailabilityEvent(string Route, ulong MessageCount);
```

---

### 4.3 RPC Domain

```csharp
public interface IRpcClient
{
    /// <summary>Sends RPC request and streams responses (async enumerable of frames).</summary>
    Task<IAsyncEnumerable<RpcResponseFrame>> CallAsync(
        string route,
        byte[] body,
        int? timeoutMs = null,
        CancellationToken cancellationToken = default);

    /// <summary>Registers a worker to handle incoming RPC requests on pattern.</summary>
    Task<IDisposable> RegisterWorkerAsync(
        string pattern,
        Func<RpcRequest, IResponseWriter, ValueTask> handler,
        CancellationToken cancellationToken = default);
}

public interface IResponseWriter
{
    /// <summary>Sends a response frame (isEnd=true marks end of stream).</summary>
    ValueTask SendAsync(byte[] body, bool isEnd = false, CancellationToken cancellationToken = default);
}

// Helper Types
public sealed record RpcRequest(string Route, byte[] Body);
public sealed record RpcResponseFrame(byte[] Body, ulong Sequence);
```

---

### 4.4 Lease Domain

```csharp
public interface ILeaseClient
{
    /// <summary>Acquires a distributed lock with TTL.</summary>
    ValueTask<ILease> AcquireAsync(
        string route,
        ulong ttlSeconds,
        int? waitSeconds = null,
        CancellationToken cancellationToken = default);

    /// <summary>Queries current lease status.</summary>
    ValueTask<LeaseInfo> QueryAsync(string route, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to lease change notifications.</summary>
    Task<IAsyncEnumerable<LeaseChangeEvent>> SubscribeAsync(
        string pattern,
        CancellationToken cancellationToken = default);
}

public interface ILease
{
    string Route { get; }
    ulong Token { get; }  // Fencing token

    /// <summary>Extends lease TTL.</summary>
    ValueTask<ulong> ExtendAsync(ulong ttlSeconds, CancellationToken cancellationToken = default);

    /// <summary>Extends lease with explicit fencing token.</summary>
    ValueTask<ulong> ExtendWithTokenAsync(ulong token, ulong ttlSeconds, CancellationToken cancellationToken = default);

    /// <summary>Alias for ExtendAsync (semantic clarity).</summary>
    ValueTask<ulong> RenewAsync(ulong ttlSeconds, CancellationToken cancellationToken = default);

    /// <summary>Releases the lease.</summary>
    ValueTask ReleaseAsync(CancellationToken cancellationToken = default);

    /// <summary>Releases with explicit fencing token.</summary>
    ValueTask ReleaseWithTokenAsync(ulong token, CancellationToken cancellationToken = default);
}

// Helper Types
public sealed record LeaseInfo(bool IsHeld, string? Owner = null, ulong? TtlRemainingSecs = null);
public sealed record LeaseChangeEvent(string Route, LeaseInfo Status);
```

---

### 4.5 Notice Domain

```csharp
public interface INoticeClient
{
    /// <summary>Publishes a notice to a route (fire-and-forget pub/sub).</summary>
    ValueTask PublishAsync(
        string route,
        byte[] body,
        CancellationToken cancellationToken = default);

    /// <summary>Subscribes to notices matching pattern (* = one level, ** = any depth).</summary>
    Task<IAsyncEnumerable<NoticeMessage>> SubscribeAsync(
        string pattern,
        CancellationToken cancellationToken = default);
}

// Helper Types
public sealed record NoticeMessage(string Route, byte[] Body);
```

---

### 4.6 Stream Domain

```csharp
public interface IStreamClient
{
    /// <summary>Begins a write session with optimistic concurrency control.</summary>
    ValueTask<IStreamSession> BeginAsync(
        string route,
        ulong expectedOffset = 0,
        byte[]? ingestMetadata = null,
        CancellationToken cancellationToken = default);

    /// <summary>Reads records from stream (async enumerable, supports backpressure).</summary>
    Task<IAsyncEnumerable<StreamRecord>> ReadAsync(
        string route,
        ulong startOffset,
        ulong limit = 1000,
        ulong? maxBytes = null,
        CancellationToken cancellationToken = default);

    /// <summary>Peeks at the latest record without consuming.</summary>
    ValueTask<StreamRecord?> PeekAsync(string route, CancellationToken cancellationToken = default);

    /// <summary>Gets metadata about the stream (bounds, record count).</summary>
    ValueTask<StreamMetadata> MetadataAsync(string route, CancellationToken cancellationToken = default);

    /// <summary>Subscribes to stream commit notifications.</summary>
    Task<IAsyncEnumerable<StreamCommitEvent>> SubscribeAsync(
        string pattern,
        CancellationToken cancellationToken = default);
}

public interface IStreamSession
{
    /// <summary>Appends a record to the stream.</summary>
    ValueTask<ulong?> AppendAsync(byte[] body, byte[]? metadata = null, CancellationToken cancellationToken = default);

    /// <summary>Commits session (finalizes appended records).</summary>
    ValueTask CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>Rolls back session (discards appended records).</summary>
    ValueTask RollbackAsync(CancellationToken cancellationToken = default);
}

// Helper Types
public sealed record StreamRecord(ulong Offset, byte[] Body, byte[]? Metadata = null);
public sealed record StreamMetadata(ulong FirstOffset, ulong LastOffset, ulong RecordCount);
public sealed record StreamCommitEvent(string Route, ulong CommitOffset);
```

---

### 4.7 Schedule Domain

```csharp
public interface IScheduleClient
{
    /// <summary>Creates a recurring schedule with cron expression.</summary>
    ValueTask<string?> CreateAsync(
        string route,
        string cronExpression,
        byte[]? payload = null,
        CancellationToken cancellationToken = default);

    /// <summary>Cancels a scheduled job.</summary>
    ValueTask CancelAsync(string route, CancellationToken cancellationToken = default);

    /// <summary>Lists scheduled jobs (paged).</summary>
    ValueTask<(ScheduleEntry[], ulong TotalCount)> ListAsync(
        ulong offset = 0,
        ulong limit = 100,
        CancellationToken cancellationToken = default);

    /// <summary>Subscribes to job execution notifications.</summary>
    Task<IAsyncEnumerable<ScheduleExecutionEvent>> SubscribeAsync(
        string pattern,
        CancellationToken cancellationToken = default);
}

// Helper Types
public sealed record ScheduleEntry(string Id, string Route, string CronExpression, byte[]? Payload = null);
public sealed record ScheduleExecutionEvent(string ScheduleId, string Route, ulong ExecutedAt);
```

---

## 5. Error Handling Strategy

### Current Exception Hierarchy ✅ (Enhance with Metadata)

```csharp
public abstract class FitzException : Exception
{
    public string ErrorCode { get; }
    public byte? ProtocolStatus { get; }
}

// Domain-Specific Sealed Exceptions
public sealed class KvException : FitzException { }
public sealed class QueueException : FitzException { }
public sealed class RpcException : FitzException { }
public sealed class LeaseException : FitzException { }
public sealed class NoticeException : FitzException { }
public sealed class StreamException : FitzException { }
public sealed class ScheduleException : FitzException { }

// Connection Exceptions
public sealed class ConnectionException : FitzException { }
public sealed class AuthenticationException : FitzException { }
public sealed class RequestTimeoutException : FitzException { }
public sealed class TransactionConflictException : KvException { }
public sealed class LeaseContendedException : LeaseException { }
public sealed class RouteNotFoundException : FitzException { }
```

### Design Decisions
- ✅ **Sealed domain exceptions** (prevent accidental derivation)
- ✅ **ErrorCode property** (programmatic error handling)
- ✅ **ProtocolStatus metadata** (diagnostic information)
- ✅ **Specific exception types** for common failure modes (timeouts, conflicts, not found)

---

## 6. Type System & Immutability

### Value Types
All result/data types use **sealed records** for:
- Immutability (init-only properties)
- Value semantics (equality by content)
- Pattern matching support
- Zero-overhead abstraction

```csharp
// Example: Sealed record with discriminator pattern
public sealed record KvGetResult(bool Found, byte[]? Value = null)
{
    // Use like: if (result.Found) { var value = result.Value!; }
    // Or pattern match: if (result is { Found: true, Value: not null } v) { }
}
```

### Interface Types
Active resources use **interfaces** for lifecycle management:
```csharp
public interface ILease { }         // Resource management
public interface IStreamSession { } // Explicit session lifecycle
public interface IKvTransaction { } // Transaction semantics
```

---

## 7. Performance Considerations

### Allocation Minimization
- ✅ **ValueTask<T>** for frequently-awaited operations (hotpath)
- ⚠️ **Struct enumerators** in LINQ (IAsyncEnumerable uses struct enumerator internally)
- ⚠️ **Buffer pooling** for binary I/O (ArrayPool<byte> for temporary buffers)
- ✅ **Sealed types** (aids JIT optimization, inlining)

### Latency Targets (PERF_GUIDELINES.md)
| Operation | Target | Implementation |
|-----------|--------|-----------------|
| KV.Get (hotpath) | <10μs | ValueTask + optimized serialization |
| Lease.Acquire (contention) | <50μs | ValueTask + minimal lock contention |
| Queue.Enqueue (fire-forget) | <5μs | ValueTask + single-frame I/O |
| Stream.Metadata (metadata) | <10μs | ValueTask + cached response |

### Throughput Targets
- **Concurrent producers** (100+): Use Channels<T> instead of locks
- **Multiplexer capacity**: 10k+ in-flight requests per client
- **Subscription fanout**: O(1) routing via DashMap

---

## 8. Backward Compatibility

### Current API Breaking Changes (from baseline)
None for existing operations (Begin, Get, Put, Commit, Rollback, Enqueue, etc.).

**New operations** added without affecting existing code:
- KV: Insert, Delete, DeleteRange, Scan
- Queue: Reserve, Subscribe, item lifecycle (Extend, Complete)
- RPC: Call streaming, RegisterWorker
- Lease: Subscribe
- Notice: Subscribe
- Stream: Peek, Subscribe
- Schedule: List, Subscribe

### Version Strategy
- **Major.Minor**: 1.0 → 2.0 when any breaking change introduced
- **Patch**: 1.0.0 → 1.0.1 for bug fixes only
- **Pre-release**: Use -alpha, -beta for feature iterations

---

## 9. Implementation Checklist

- [ ] Port all 7 domain clients with complete operation set
- [ ] Implement IAsyncEnumerable subscriptions for all event-driven domains
- [ ] Update exception types with specific error scenarios
- [ ] Refactor hotpath operations to use ValueTask<T>
- [ ] Add comprehensive unit/integration tests (target >80% coverage)
- [ ] Validate <10μs latency on hotpath benchmarks
- [ ] Document API surface in README and inline XML comments

---

## 10. References

- **Protocol**: [fitz/docs/clients/client-spec.md](../../fitz/docs/clients/client-spec.md)
- **Performance**: [PERF_GUIDELINES.md](./PERF_GUIDELINES.md)
- **Reference Implementations**: [fitz-go](../../fitz-go), [fitz-py](../../fitz-py), [fitz-ts](../../fitz-ts)
- **.NET Idioms**: [Microsoft Async Guidelines](https://docs.microsoft.com/en-us/archive/msdn-magazine/2013/march/async-await-best-practices-in-asynchronous-programming)
