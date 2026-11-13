# Changelog

## 4.2.0 (unreleased)

This release deliberately breaks the public API to remove Wire 2 leftovers and make the
surface more idiomatic. The package stays on 4.x.

### Breaking changes

- Renames `Client` to `FitzClient` and `IClient` to `IFitzClient`.
- Removes `ClientConfig.AuthSettleDelay`, which has had no effect since Wire 3.
- Replaces `IQueueClient.EnqueueWithOptionsAsync` with an `EnqueueAsync(route, body, QueueEnqueueOptions, ct)`
  overload. `QueueEnqueueOptions.TtlHours` becomes `TimeToLive`, a `TimeSpan` that must be a positive
  whole number of hours.
- `QueueAvailabilityEvent` exposes typed `Ready`, `Delayed`, and `InFlight` counts instead of raw `Payload` bytes.
- Removes `QueueItem.Attempt`, `IQueueReservedItem.Attempt`, and `QueueItem.AttemptUnavailable`; the wire
  never reported an attempt count.
- Collapses Schedule listing into one cursor API: `ListAsync(continuation, limit)` returns
  `ScheduleListPage(Entries, HasMore, Continuation)`. `ListV2Async`, `ScheduleCursorPage`, the offset
  parameter, and `ScheduleListPage.TotalCount` are removed.
- `LeaseListItem.AcquiredAt` is a `DateTimeOffset` parsed from the broker's RFC 3339 timestamp. An
  unparseable value fails the page with `LIST_INVALID_RESPONSE`.
- `IKvTransaction.Route` is a required member instead of a default that throws.
- Invalid client-side routes and patterns throw `ArgumentException` (with `ParamName`) instead of a domain
  exception carrying `INVALID_ROUTE`. Broker-reported route errors are unchanged.
- A closed `FitzClient` throws `ObjectDisposedException` instead of a retryable `ConnectionException`.
- `FitzException` moves to `Cntryl.Fitz.Abstractions` (same namespace); `ErrorClass` and `ErrorEffect` are
  init-only. `SubscriptionBackpressureException` and `AsyncHandlerOverflowException` now derive from
  `FitzException`.
- `ILeaseClient.AcquireAsync` and `LeaseExecutionOptions.Wait` reject waits above the broker's 30-second cap
  with `ArgumentOutOfRangeException`. `IQueueClient.ReserveAsync` rejects batches above 1,024.
- `KvDirectory`: `Page<T>` becomes `KvDirectoryPage<T>`, `DeleteAsync` becomes `DeleteByIdAsync`, and
  directories are built with `KvDirectory.Define(...)`.
- Dependency injection: `AddFitzClient` connects with `ConnectWhenReadyAsync` by default (configurable through
  `FitzClientHostingOptions.Startup`) and throws when a client is registered twice.
- Analyzers FITZ001–FITZ004 report errors instead of warnings.
- Every domain reads broker responses the same way. A response with an unknown status byte now fails with
  `{OPERATION}_INVALID_RESPONSE` (was `{OPERATION}_FAILED` for Lease, Notice, Schedule, Stream, and RPC), and a
  failed `ENQUEUE` message reads `ENQUEUE failed: …` like every other operation.

### Fixes and improvements

- Lease acquisition follows Wire 3: a queued ACQUIRE is one correlated request whose grant, timeout, or error
  arrives on the final frame. The multiplexer stops the response timeout once a non-final frame arrives.
- `WithLeaseAsync` cancels the callback's token when the connection drops, even while the callback is busy
  with synchronous work, and retries a renewal the broker definitely rejected while the lease still has slack.
- An explicit RPC call budget is no longer cut short by the default response timeout.
- Ordered `SemaphoreSlim` shutdown for subscription handles, worker registrations, KV transactions, stream
  sessions, lease handles, and domain clients: disposal drains in-flight operations before releasing resources.
- Records malformed or undeliverable notifications on the `fitz.notification.dropped` counter.
- Rejects missing or repeated cursors while listing schedules by selector, instead of paging forever.
- Reads stream commit offsets from top-level response properties only.
- Releases the socket when a TCP connect fails partway.
- Request retry and reconnect share one jittered exponential backoff.
- Disconnect listeners and session-token cancellation callbacks no longer run while the connection lock is
  held, so a callback can call back into the client. A reconnect attempt waits for the lost session's teardown
  to finish, and a session is reported authenticated only while its transport is still the current one.
- Removes all analyzer suppressions from shipped code.
- Stream, Schedule, Lease, and Notice now count malformed NOTIFY frames on `fitz.notification.dropped`
  instead of dropping them silently.
- Internal: one subscription table serves all six notification domains, one response-envelope reader
  serves every domain, session-bound handles share one disconnect latch, and connection telemetry and
  listener registries move out of `FitzConnection`. RPC splits into a caller, a worker host, and a wire
  codec; managed lease execution moves into its own scope type.
- A duplicate inbound RPC correlation id is ignored instead of throwing inside notification dispatch.
- `RpcRequest` takes an optional `TimeProvider` that counts down `RemainingTime`, and every subscription
  handle (`SubscriptionHandle`, and the KV, Queue, Stream, Schedule, Lease, and Notice subscriptions) takes one
  that bounds its five-second unsubscribe on disposal. The system clock remains the default.
- Internal: the connection, multiplexer, domain clients, and their sessions keep time through one injected
  `TimeProvider`, so retry and reconnect backoff, request and handler timeouts, lease renewal and inventory
  timing, RPC budgets and the cancellation window, and session cleanup budgets are tested on a manual clock
  without real delays.

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
