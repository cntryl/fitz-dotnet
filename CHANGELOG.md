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

- Preserved structured error codes across all stream operations.

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
