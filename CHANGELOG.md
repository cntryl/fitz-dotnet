# Changelog

All notable changes to the Fitz .NET client packages are recorded here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This project is in preview (`0.x`). Per [Semantic Versioning](https://semver.org/),
breaking changes may land in any `0.x` release; each one is called out below.

**Package version and assembly version are deliberately decoupled.** `PackageVersion`
advances with every release; `AssemblyVersion` stays pinned at `1.0.0.0` so binding
never breaks for consumers. `test/Fitz.PackageConsumer` asserts this on every CI run.

## [Unreleased]

### Changed

- **Breaking (source):** no `ILeaseClient` member throws `NotSupportedException` any more, and
  the three `public const string` fields that froze those messages as API are gone. Four
  members were default interface methods that threw, so holding an `ILeaseClient` told you
  nothing about whether `ListAsync`, `ObserveAsync`, or the authority-aware `WithLeaseAsync`
  overloads would work. `ListAsync` and `ObserveAsync` are now required. The two
  authority-aware `WithLeaseAsync` overloads became the required primitives, and the
  cancellation-only pair now has a real default that delegates to them and discards the fence —
  so a custom implementation writes two overloads instead of four, and every member works.
  DIMs that throw are the right tool for adding members to a shipped `0.x` interface; 1.0 is
  where that debt gets paid rather than frozen.
- **Breaking (source):** every duration on the public surface is a `TimeSpan`. The API
  previously mixed three integer types and two units across four naming conventions —
  `ttlSecs` and `leaseSeconds` (`ulong`), `waitSeconds` (`uint` on `AcquireAsync` but `int?`
  on `ReserveAsync`), `ExpiresInSecs`, `TtlRemainingSecs`, `LeaseExecutionOptions.WaitSeconds`
  (`uint`), and `delayMs` (`int?`, milliseconds) — while connection-level options already used
  `TimeSpan`. Renamed accordingly: `ttlSecs`/`leaseSeconds` to `ttl`/`lease`, `waitSeconds` to
  `wait`, `delayMs` to `delay`, `WaitSeconds` to `Wait`, `ExpiresInSecs` to `ExpiresIn`, and
  `TtlRemainingSecs` to `TtlRemaining`.
  A duration finer than the wire can carry is rejected with `ArgumentOutOfRangeException`
  rather than rounded; rounding would silently shorten a lease. This also retires the worst
  edge in the old API, `EnqueueAsync(delayMs)`, which took milliseconds but threw on any value
  that was not a whole multiple of 1000.
  The analyzer now evaluates `TimeSpan.Zero` and the `TimeSpan.From*` factories over constants,
  so these stay compile-time errors instead of becoming runtime ones.
- **Breaking (source):** `ScheduleDeliveryMode.Single` is renamed `Once`. `Single` reads as the
  `System.Single` alias in C#; the wire value is unchanged. This diverges from the name the
  other Fitz clients use, deliberately, because it is the correct name in this language.
- **Breaking (behaviour):** `StreamFilteredReason` gains `None = 0`, and a filtered read item
  whose wire reason is 0 now decodes to `None` rather than `null`. Previously "filtered, but
  the broker named no reason" and "not a filtered entry at all" were both `null` and could not
  be told apart. A null `StreamReadItem.Reason` now means only the latter.
- Every assembly-level suppression is gone, and `GlobalSuppressions.cs` with them. Each was
  removed by fixing what it hid rather than by relocating it: the `byte[]` properties, the
  `byte`-backed enums, and the two enum-shape findings above. `.editorconfig` now states that
  namespaces deliberately do not mirror folders, which also retired two `NoWarn` entries.

### Added

- [docs/guide.md](docs/guide.md), a consumer guide covering install and connect, routes,
  cancellation, errors and retry, subscriptions, reconnect semantics, all seven domains,
  dependency injection, and observability. Every code sample in it is compiled against the
  packed packages rather than written by hand.
- **Breaking (source):** the entire public surface moved into the single `Cntryl.Fitz`
  namespace. `Cntryl.Fitz.Abstractions`, every `Cntryl.Fitz.Abstractions.Domains.*`, and the
  public halves of `Cntryl.Fitz.Errors`, `Cntryl.Fitz.Transport`, `Cntryl.Fitz.Observability`
  and `Cntryl.Fitz.Runtime` are gone as namespaces; assembly and package names are unchanged,
  as is `Cntryl.Fitz.DependencyInjection`. Replace the per-domain using directives with
  `using Cntryl.Fitz;` — `test/Fitz.PackageConsumer` went from eight to two. No type was
  renamed, and all 101 public type names were confirmed collision-free before the move.
- **Breaking (source):** `StreamRecord.Body`, `StreamRecord.Metadata`, and
  `ScheduleEntry.Payload` are `ReadOnlyMemory<byte>` instead of `byte[]`, matching `KvPair`
  and `KvGetResult` and no longer handing callers a mutable reference to record state. Call
  `.Span` or `.ToArray()` at the use site.
- **Breaking (source):** `KvMode`, `KvDurability`, and `ScheduleDeliveryMode` no longer
  declare `byte` storage. The wire encoding is unchanged — those paths already cast
  explicitly — but the public contract no longer states a storage size it does not owe.
- `ConnectWhenReadyAsync` now attaches the failure that caused a startup timeout as the
  `TimeoutException`'s inner exception instead of discarding it, and the two connection-loss
  log events that swallow their exception now carry full detail rather than only `Message`.
- **Breaking (source):** the connection, protocol, and measurement internals are no longer
  public. `FitzConnection`, `Multiplexer`, `FrameCodec`, `FrameParser`, `Frame`,
  `MessageTypes`, `ServerCapabilities`, `BinaryBufferReader`, `BinaryBufferWriter`,
  `PerfTimer`, `PerfSummary`, `LatencyHistogram`, `ThroughputMeter`, and the concrete
  domain clients (`KvClient`, `KvTransaction`, `LeaseClient`, `LeaseHandle`, `NoticeClient`,
  `QueueClient`, `RpcClient`, `ScheduleClient`, `StreamClient`, `StreamSession`) are now
  `internal`. Reach every one of them through `Client` and the `Cntryl.Fitz.Abstractions`
  interfaces, which are unchanged. This shrinks the exported surface of `Cntryl.Fitz` from
  57 types to 34 and is the last practical moment to do it: `AssemblyVersion` is pinned at
  `1.0.0.0`, so anything left public here is public permanently.
  The transport extension point is untouched and stays public: `ITransport`, `PooledFrame`,
  `TransportResolver`, `TcpTransport`, and `WebSocketTransport`.

### Added

- `FitzLimits.MinFrameSize` and `FitzLimits.MaxFrameSize` publish the protocol bounds that
  `ClientConfig.MaxFrameSize` is validated against, so a caller can check a configured frame
  size before `Validate` runs. They replace the reachability that internalizing `FrameCodec`
  removed; `ClientConfig` now states its default and its validation in the same terms.
- **Breaking (source):** the cancellation parameter is now named `ct` across the entire
  public surface. The domain clients already used `ct`; `IClient.ConnectAsync`,
  `ConnectWhenReadyAsync`, and `CloseAsync` used `cancellationToken` and have been
  renamed to match. Positional calls are unaffected; callers passing it by name on those
  three methods must update `cancellationToken: token` to `ct: token`. `ct` is the
  project's standard — see CONTRIBUTING.md.
- Transports now report a diagnostic label through the new `ITransport.TransportName`.
  The built-in transports return `"WebSocketTransport"` and `"TcpTransport"` as before;
  a caller-supplied transport that does not override the property now reports `"custom"`
  instead of its runtime type name. Added as a default interface member, so existing
  `ITransport` implementations continue to compile unchanged.
- `AddFitzClient` registers every service through an explicit factory instead of the
  container's type-based activation, removing constructor reflection from startup.

### Fixed

- Removed a latent trimming/Native AOT defect present in `0.1.3`: the internal
  `Client.GetDomain<T>(Lazy<T>)` helper propagated `Lazy<T>`'s
  `PublicParameterlessConstructor` requirement onto an unannotated type parameter
  (`IL2091`). No reflection actually occurred, but the contract forced the trimmer to
  preserve constructors nothing calls. Domain properties now dereference their own
  `Lazy<T>` fields directly.

### Removed

- All remaining runtime reflection from the shipped packages: `Enum.IsDefined`, enum
  `ToString`/interpolation, and `GetType()`. See
  [docs/aot-and-reflection.md](docs/aot-and-reflection.md) for the standing contract.

### Added

- XML documentation for the entire public API of all three runtime packages, shipped in
  the NuGet packages so consumers get IntelliSense. `GenerateDocumentationFile` is on and
  warnings are errors, so a missing doc comment now fails the build.

### Documentation

- Added [CONTRIBUTING.md](CONTRIBUTING.md) and this changelog.
- Added [docs/aot-and-reflection.md](docs/aot-and-reflection.md), the trimming and
  Native AOT contract, its CI enforcement, and the rules that keep it true.
- Corrected `PERF_GUIDELINES.md`, which mandated `ValueTask` for public one-shot
  operations that ship as `Task`, cited a `.NET 9` throughput baseline and an allocation
  budget that were never measured for this client, and linked `PERF_BENCHMARKS.md` and
  `bench_results/` — neither of which exists.

### Internal

- The Native AOT verification in CI now roots all three shipped assemblies, reports each
  trim/AOT finding individually, and fails on any of them. Previously it analyzed only
  the code the sample consumer's `Main` reached, which is why the `IL2091` above went
  unnoticed. The consumer also restores into a dedicated package folder that CI clears,
  so a stale copy of a fixed package version cannot shadow freshly packed artifacts.

## [0.1.3]

### Fixed

- Hardened pooled-buffer ownership, malformed-frame handling, reconnect and request
  correlation, bounded subscription delivery, retryable cleanup, stream pagination,
  hosted DI lifecycle, and RPC response dispatch, preserving wire compatibility.
- Aligned RPC with the frame-level correlation protocol.
- Corrected stream subscription response decoding.

### Added

- Guaranteed Native AOT compatibility across the shipped packages.

## [0.1.2]

### Fixed

- Preserved structured error codes across all stream operations. Stream status 2 decodes as
  `[u32 BE domain_code][string message]` and surfaces through `StreamException.DomainCode`,
  including 2001 for `APPEND` and `COMMIT`. Legacy status 1 remains supported and leaves
  `DomainCode` null. Classify optimistic-concurrency failures on code 2001 and backend
  failures on 2012, never on message wording. No automatic command retry was introduced, and
  success and notification layouts are unchanged.
- Deploy order matters for this one: clients that decode the new envelope must be released
  ahead of the broker that emits it, and a rollback restores the broker first.

### Changed

- **Breaking (binding):** all three assemblies moved to the permanent `AssemblyVersion`
  `1.0.0.0`, from `0.1.1.0`. Every later release advances `PackageVersion` alone, and CI
  asserts the assembly identity of all three packages on every run.

## [0.1.1]

### Added

- Patterned lease inventory observation.

### Fixed

- Hardened lease inventory observer recovery.
- Surfaced subscription handler overflow to callers.

## [0.1.0]

Initial preview release: KV, Queue, RPC, Lease, Notice, Stream, and Schedule domains over
WebSocket and TCP, with reconnect, retry, bounded subscriptions, observability hooks,
Roslyn route analyzers, and dependency injection extensions.

The preview settled these source and wire contracts, several of which broke the shapes used
by pre-release builds:

- KV callers pass durability explicitly, ahead of the optional mode:
  `BeginAsync(route, KvDurability.Async, KvMode.ReadWrite, ct)`.
- `CloseAsync` is the explicit idempotent shutdown; `DisposeAsync` delegates to it.
- `ILease` is `IAsyncDisposable`; use `await using` so a live lease is released once.
- RPC worker callbacks return `ValueTask`; return `ValueTask.CompletedTask` when synchronous.
- Schedule enumeration uses `ListAsync(offset, limit)` and returns `ScheduleListPage.Entries`
  plus `TotalCount` on canonical wire message 702.
- KV `ScanAsync` returns `KvScanResult`, carrying key/value pairs and `HasMore`.
- Queue notifications expose the broker-defined length-prefixed `Payload`; the invented
  ready/delayed/inflight counters were removed.
- Stream records expose `GlobalOffset` for global selectors, and `BEGIN`/`APPEND` accept only
  their canonical response layouts.
- Lease queries expose `PendingWaiters`, and queued acquisition follows the broker's
  deferred-acquisition flow.
- Managed lease callbacks may accept `(LeaseAuthority authority, CancellationToken ct)` to
  receive the immutable admission fencing token; cancellation-only callbacks still compile.
- Stream global continuation reuses the returned fingerprint and captured-watermark pair.
- Frame parsing is strict: trailing or truncated data is a protocol error, which callers
  observe as `ProtocolException`.
