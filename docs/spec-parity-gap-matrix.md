# fitz-dotnet Spec/Parity Gap Matrix

Date: 2026-08-02

Status legend:

- `implemented`: represented in the public surface and backed by current tests or conformance artifacts
- `historical`: older audit trail retained elsewhere; not current truth

Current status: `fitz-dotnet` is aligned to the shared 17-scenario Fitz client suite and has separately passed the four-client operational parity review. Shared-suite success alone does not establish explicit durability, managed leases, safe retry, reconnect defaults, heartbeat, observability, error ergonomics, or documentation truth; those capabilities require the independent evidence below.

| Capability | .NET surface / evidence | Shared contract coverage | Status | Notes |
| --- | --- | --- | --- | --- |
| Shared conformance runner | `tests/Core/Integration/ConformanceSmokeTests.Runner.cs`, `ConformanceModels.cs`, `conformance/cross-language-conformance-suite.yaml` | `CS-001` to `CS-017` | `implemented` | Runner enforces suite completeness against the local YAML and emits the shared artifact schema |
| Transport and auth matrix | `ClientTransport.Auto`, `TransportResolver`, `.github/workflows/ci.yml`, `compose.yml` | WebSocket, TCP, anonymous, valid JWT, invalid JWT | `implemented` | CI retains one artifact per `websocket|tcp` x `anonymous|valid_jwt` leg and separately checks invalid JWT auth failure |
| Connect lifecycle and startup readiness | `IClient.ConnectAsync`, `IClient.ConnectWhenReadyAsync`, `Client`, `FitzConnection` | `CS-001`, `CS-002` | `implemented` | Startup retry is bounded, auth settles on transport survival instead of probe traffic, and concurrent connect callers coalesce |
| Same-client reconnect path | `FitzConnection`, `Client.ConnectWhenReadyAsync`, `tests/Core/Integration/ConformanceSmokeTests.cs` | `CS-009`, `CS-010`, `CS-015` | `implemented` | Broker-backed `CS-010` forces a live disconnect, waits for same-client recovery, checks stale-handle invalidation, and proves post-reconnect requests succeed |
| Retry execution and request classification | `RetryOptions`, `RetryOperation`, connection retry path, domain retry hooks | `CS-003` to `CS-008`, `CS-010` | `implemented` | Replayable reads retry centrally; session-bound mutations and auth paths stay terminal |
| Request gating and bounded queueing | `RequestGate`, `RequestQueueFullException`, `ClientConfig.MaxRequestQueueSize` | `CS-014`, `CS-017` | `implemented` | In-flight concurrency and waiter saturation are bounded and covered by unit plus conformance tests |
| Heartbeat and silent transport loss handling | `HeartbeatOptions`, transport keepalive wiring, `FitzConnection` idle watchdog | `CS-009`, reconnect runtime coverage | `implemented` | Heartbeat timeout is treated as a reconnect-worthy transport failure |
| Async handler execution controls | `AsyncHandlerOptions`, `AsyncHandlerDispatcher`, subscription and RPC worker dispatch | subscription and worker reconnect/runtime behavior | `implemented` | Handler concurrency and timeout are shared across subscriptions and RPC workers |
| Observability primitives | `src/Core/Observability/FitzObservability.cs`, lifecycle hooks in `FitzConnection` | lifecycle, retry, timeout, saturation evidence | `implemented` | Core stays dependency-light and exposes logger/tracer/meter/lifecycle callbacks |
| Domain parity | KV, Queue, Notice, RPC, Lease, Schedule, Stream abstractions and runtime clients | shared acceptance criteria plus `CS-003` to `CS-017` | `implemented` | Public APIs now cover the same runtime features tracked in TS for the shared suite |
| Repo-owned broker baseline and docs | `compose.yml`, `README.md`, this matrix | local and CI verification flow | `implemented` | The repo no longer depends on sibling checkout paths for conformance or broker setup |

## Open Gaps

None are tracked against the current shared suite. Historical findings remain in [spec-parity-audit.md](spec-parity-audit.md) for context, but they should not be read as current status.
