# Changelog

## 4.1.2 (unreleased)

- Decodes Queue availability notifications using the broker's three fixed-width counters; `Payload` contains the 24 counter bytes in wire order.
- Buffers early notifications while the first subscription installs its routing state across KV, Queue, Stream, Notice, Schedule, and Lease.
- Faults subscription handles with `SubscriptionBackpressureException` and resets the connection when the shared notification queue overflows. Applications must create new subscriptions after that terminal delivery failure.
- Prevents notifications queued by an old transport session from being dispatched after reconnect.
- Isolates activation overflow by subscription ID, rejects subscription publication after disposal, and releases enumeration ownership even when an enumerator is disposed before its first read.
- Aborts interrupted TCP frame writes, isolates throwing cancellation callbacks during teardown, and reclaims canceled admission waiters.
- Bounds canceled-request correlation history to 1,024 identifier-only tombstones; exceeding that limit resets the session instead of retaining unbounded request objects or evicting late-response identities.
- Schedules overlapping RPC workers against available local capacity with bounded, cancellable waits and elapsed-budget accounting. Calls observe the lifetime of their actual sending session.
- Binds new transaction, stream-session, lease, and reserved-item handles to the session that returned them. Uncertain lease renewal closes the low-level handle.

## 4.1.1 (unreleased)

- Enables .NET 11 runtime-async compilation across projects.

## 4.1.0 (unreleased)

- Targets .NET 11 and adopts `Fitz.slnx`.
- Updates NuGet dependencies and applies Cntryl conventions across all projects.
- Centralizes project settings and streamlines documentation.
- Requires Fitz 0.4.0 / Wire 3, including negotiated Queue TTL and NACK support.

See [Wire 3 migration](docs/wire3-migration.md) for broker compatibility and Queue behavior.
Earlier release notes are available in [Git history](https://github.com/cntryl/fitz-dotnet/commits/main/CHANGELOG.md).
