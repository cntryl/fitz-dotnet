# Unreleased API migration

- KV callers must pass durability explicitly before the optional mode:
  `BeginAsync(route, KvDurability.Async, KvMode.ReadWrite, cancellationToken)`.
- Call `CloseAsync` for explicit idempotent shutdown. `DisposeAsync` delegates to it.
- `ILease` is now `IAsyncDisposable`; use `await using` so a live lease is released once.
- RPC worker callbacks now return `ValueTask`; return `ValueTask.CompletedTask` for synchronous completion.
- Schedule enumeration now uses `ListPageAsync` and its continuation token (wire message 707).
- Stream global continuation reuses the returned fingerprint and captured-watermark pair.
- Frame parsing is strict; callers should use `FrameCodec.DecodeStrict` and handle trailing or truncated data as protocol errors.

These are source-level breaks only. This change does not alter package versions or wire IDs.
