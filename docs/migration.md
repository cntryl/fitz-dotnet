# Unreleased API migration

- KV callers must pass durability explicitly before the optional mode:
  `BeginAsync(route, KvDurability.Async, KvMode.ReadWrite, cancellationToken)`.
- Call `CloseAsync` for explicit idempotent shutdown. `DisposeAsync` delegates to it.
- `ILease` is now `IAsyncDisposable`; use `await using` so a live lease is released once.
- RPC worker callbacks now return `ValueTask`; return `ValueTask.CompletedTask` for synchronous completion.
- Schedule enumeration now uses `ListAsync(offset, limit)` and returns `ScheduleListPage.Entries` plus `TotalCount` on canonical wire message 702.
- KV `ScanAsync` returns `KvScanResult` containing key/value pairs and `HasMore`.
- Queue notifications expose the broker-defined length-prefixed `Payload`; the invented ready/delayed/inflight counters were removed.
- Stream records expose `GlobalOffset` for global selectors, and BEGIN/APPEND accept only their canonical response layouts.
- Lease queries expose `PendingWaiters`; queued acquisition follows the broker deferred-acquisition flow.
- Stream global continuation reuses the returned fingerprint and captured-watermark pair.
- Frame parsing is strict; callers should use `FrameCodec.DecodeStrict` and handle trailing or truncated data as protocol errors.

These source and wire contract breaks are reflected in package version 0.2.0.
