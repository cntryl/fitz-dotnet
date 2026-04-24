# Fitz-Dotnet Spec/Parity Audit

Date: 2026-04-24
Audience: fitz-dotnet maintainers and Fitz client platform owners
Scope: internal parity audit against the shared Fitz client contract

## Summary

This audit evaluates `fitz-dotnet` against the shared Fitz client contract in `../fitz/docs/clients`, not just against local unit-test health.

Status note: this is a historical audit snapshot; the current implementation status is tracked in [docs/spec-parity-gap-matrix.md](docs/spec-parity-gap-matrix.md).

Baseline facts used in this audit:

- `dotnet test tests/Core/Core.Tests.csproj` passed with 125 tests on 2026-04-23.
- `fitz-dotnet` now has a shared conformance runner that covers `CS-001` through `CS-015` and writes normalized JSON artifacts.
- The shared cross-language suite defines `CS-001` through `CS-015`.
- `fitz-go` and `fitz-py` already expose dedicated conformance targets aligned to the shared runner contract.
- `fitz-dotnet` currently supports WebSocket and TCP transports, but shared-suite coverage is still websocket-heavy.

Overall assessment:

- Core request/response plumbing exists for a subset of Fitz behavior.
- The repository is not yet spec-complete for Fitz client parity.
- The highest-risk gaps are public-surface incompleteness, auth-state correctness, conformance-runner drift, and incomplete TCP coverage in the shared suite.

## Findings

| Severity | Finding | Contracts affected | Recommended order |
| --- | --- | --- | --- |
| P1 | Shared conformance runner exists, but matrix-specific CI wiring and transport/auth fan-out still need finishing | Runner contract, `CS-001` to `CS-015` | 1 |
| P1 | Connection auth now uses an immediate broker probe and surfaces typed auth failure, but broker-backed proof across transports is still pending | `AC-CONN-002`, `AC-CONN-003`, `AC-CONN-005`, `CS-001`, `CS-002` | 2 |
| P0 | The current public domain surface does not represent several required Fitz capabilities | Required domains contract, `AC-QUEUE-*`, `AC-NOTICE-*`, `AC-RPC-*`, `AC-SCHEDULE-*`, `AC-STREAM-010` to `AC-STREAM-014` | 3 |
| P1 | TCP transport exists, but the shared suite still needs broader proof across both transports | Suite required transport matrix, `CS-001` to `CS-015` | 4 |
| P1 | Reconnect/backoff and connection-scoped state restoration need fuller contract coverage | `AC-CONN-006`, `CS-009`, `CS-010`, `CS-015` | 5 |
| P1 | In-flight response correlation depends on FIFO-by-message-type instead of explicit request identity | `CS-014`, `AC-RPC-002`, `AC-RPC-005` | 6 |
| P2 | Error/reporting shape and repo documentation lag behind parity needs | `CS-004` to `CS-008`, runner aggregate/result shape, release auditability | 7 |

### P1. Shared conformance runner exists, but matrix-specific CI wiring still needs finishing

Current evidence:

- `tests/Core/Integration/ConformanceSmokeTests.Runner.cs` parses `cross-language-conformance-suite.yaml`, executes the `CS-001` through `CS-015` scenario set in order, and writes a normalized JSON artifact.
- `tests/Core/Integration/ConformanceSmokeTests.cs` now delegates to the shared runner instead of hardcoding the scenario list inline.
- `tests/Core/Integration/ConformanceModels.cs` now carries structured evidence and the suite metadata needed by the shared runner contract.
- The remaining work is matrix-level CI fan-out and any future tightening needed to keep the emitted artifact perfectly aligned with the cross-language harness contract.

Expected behavior:

- `fitz-dotnet` should expose a dedicated conformance target that can run the shared suite across supported transports and auth modes and emit machine-comparable results.

Likely root cause area:

- The repo now has the shared harness in place, but the matrix wiring and artifact retention story still need to be finished.

Recommended remediation:

1. Wire the runner into the full CI transport/auth matrix.
2. Keep the emitted artifact shape aligned if the shared contract evolves.
3. Retain artifacts for every supported matrix leg.

### P1. Connection auth now uses an immediate broker probe and surfaces typed auth failure, but broker-backed proof across transports is still pending

Current evidence:

- `src/Core/Connection/FitzConnection.cs` sends `CONNECT` and immediately probes the broker with a lightweight lease query before marking the session authenticated.
- Invalid JWTs now surface through `AuthenticationException` during connect rather than being treated as a successful session.
- The receive loop still converts transport failures into `Disconnected` state, so broker-backed proof is still needed across both transports.
- `tests/Core/Integration/ConformanceSmokeTests.cs` still needs end-to-end conformance coverage for the auth matrix even though the unit-level connect path is now stricter.

Expected behavior:

- `ConnectAsync` should not report success until the broker has accepted the session.
- Invalid JWTs should surface as authentication failures, not as delayed disconnects or ambiguous partial outcomes.
- Domain traffic should remain blocked until auth has definitively completed.

Likely root cause area:

- The connect state machine needed protocol confirmation rather than delay-based success; that fix is now in place, but the broker-backed matrix still needs proof.

Recommended remediation:

1. Add end-to-end conformance coverage for anonymous, valid JWT, and invalid JWT auth flows.
2. Verify the new probe-based connect path across both WebSocket and TCP.
3. Keep the docs honest if broker behavior reveals a remaining edge case.

### P0. The current public domain surface does not represent several required Fitz capabilities

Current evidence:

- `src/Abstractions/Domains/Queue/IQueueClient.cs` exposes enqueue only; reserve, complete, extend, and redelivery flows are not representable.
- `src/Abstractions/Domains/Notice/INoticeClient.cs` exposes publish only; subscribe, wildcard subscribe, unsubscribe, and client-side multiplexing are not representable.
- `src/Abstractions/Domains/Rpc/IRpcClient.cs` exposes a request method that returns `Task`, not an RPC response payload; worker registration, request handling, unregister, and chunked response semantics are absent from the public API.
- `src/Abstractions/Domains/Schedule/IScheduleClient.cs` exposes create/cancel only; list and notification subscription semantics are absent.
- `src/Abstractions/Domains/Stream/IStreamClient.cs` exposes begin/read/metadata, but the read shape is a one-shot async enumerable and does not model subscription lifecycle, observable completion, or mid-flight server errors.
- `src/Abstractions/Domains/Kv/IKvTransaction.cs` supports get/put/commit/rollback only; delete, insert-existing protection, and scan/reverse-scan acceptance criteria are not expressible.

Expected behavior:

- The public .NET abstractions should be able to express every required Fitz domain behavior that the shared contract marks as required for clients.

Likely root cause area:

- The current SDK surface was built around early happy-path operations before full Fitz domain parity was designed.

Recommended remediation:

1. Expand the abstractions layer before adding more domain-specific tests so missing behavior is representable.
2. Define parity targets per domain from the Fitz acceptance criteria, then add API surface in that order: RPC, Queue, Notice, Schedule, Stream, KV parity gaps.
3. Treat worker/subscription APIs as first-class connection-scoped capabilities rather than add-ons.

### P1. TCP transport exists, but the shared suite is still websocket-only

Current evidence:

- `src/Core/Transport/TransportResolver.cs` resolves both `"ws"`/`"websocket"` and `"tcp"`.
- `src/Core/Transport/TcpTransport.cs` exists and has unit coverage, but the shared suite still runs websocket-only.
- The shared suite marks both `websocket` and `tcp` as required transports.
- The shared runner guidance expects every client to be executable across both transports where the contract lists them.

Expected behavior:

- The client should exercise TCP in the shared runner so transport parity stays enforced.

Likely root cause area:

- The transport implementation exists; the missing piece is shared-suite coverage.

Recommended remediation:

1. Extend the future conformance runner to exercise WebSocket and TCP.
2. Keep TCP transport coverage in the unit and integration suites.
3. Only claim parity once both transport legs are covered by tests.

### P1. Reconnect/backoff and connection-scoped state restoration need fuller contract coverage

Current evidence:

- `src/Core/ClientConfig.cs` now has reconnect/backoff configuration alongside timeout and auth settings.
- `src/Core/Connection/FitzConnection.cs` performs reconnect with backoff and `OnReconnect`-based state restoration, and the close-during-backoff regression is covered.
- The public API and domain clients expose reconnect-aware behavior, but the shared suite still lacks broad disconnect/reconnect/shutdown coverage.
- The shared suite and contract still require reconnect behavior, reconnect backoff, and rebuilding connection-scoped subscriptions/workers.

Expected behavior:

- When configured, the client should detect disconnects, reconnect, send `CONNECT` first on the new transport, and restore connection-scoped behavior.

Likely root cause area:

- The reconnect path exists; the remaining gap is broader contract coverage for reconnect and shutdown scenarios.

Recommended remediation:

1. Expand conformance coverage for disconnect, reconnect, and shutdown-active-work scenarios.
2. Keep domain-scoped restoration tests aligned with `OnReconnect` registrations.
3. Refresh parity docs and runner outputs as reconnect scenarios are promoted into the shared suite.

### P1. In-flight response correlation depends on FIFO-by-message-type instead of explicit request identity

Current evidence:

- `src/Core/Connection/Multiplexer.cs` stores pending requests in queues keyed only by `messageType`.
- Same-type requests are matched by FIFO order, not by a request identifier.
- `tests/Core/Unit/MultiplexerTests.cs` codifies the FIFO assumption for two in-flight requests of the same message type.
- `src/Core/Domains/Rpc/RpcClient.cs` generates a 16-byte `correlationId`, but the response-dispatch layer does not use it for correlation.

Expected behavior:

- Responses should correlate to the originating request context, especially when multiple same-type requests are in flight.

Likely root cause area:

- The current multiplexer assumes transport/protocol ordering is sufficient.

Recommended remediation:

1. Confirm the Fitz wire contract for request identity per domain.
2. Move correlation from message-type FIFO queues to explicit request tokens where the protocol provides them.
3. Add concurrent same-domain conformance coverage before relying on higher client-side concurrency.

### P2. Error/reporting shape and repo documentation lag behind parity needs

Current evidence:

- Domain exceptions carry `Code` and `Status`, but there is no retryable/terminal classification even though `CS-006` expects that dimension to be available.
- The current .NET conformance aggregate omits shared summary fields and normalized notes expected by the runner contract.
- `examples/README.md` and the previous `docs/README.md` were placeholders, and the repo does not yet document a contract-compliant conformance invocation.

Expected behavior:

- The client should emit normalized audit/conformance artifacts and document how parity is evaluated.

Likely root cause area:

- SDK ergonomics and auditability have not yet caught up with the evolving shared Fitz contract.

Recommended remediation:

1. Introduce a richer error model or classification helper.
2. Align conformance output with the shared schema.
3. Keep internal docs current as runner support lands.

## Public Interface Impact Assessment

Remediation will require public-surface decisions in these areas:

- `ClientConfig`: already includes reconnect/backoff controls; remaining public-surface decisions are transport selection that can model TCP explicitly and, if needed, restoration knobs.
- `Client` and possibly `IClient`: likely need better connection-state observability if reconnect and auth state become first-class behavior.
- Domain abstractions: definitely need expansion for Queue, Notice, RPC, Schedule, Stream, and KV parity gaps where the current interfaces cannot express required Fitz operations.
- Conformance artifacts: the test harness/result model should align to the shared runner contract rather than the current smoke-specific shape.

## Remediation Backlog

### 1. Conformance Harness Parity

- Add a dedicated `.NET` conformance target separate from the current smoke integration tests.
- Execute all scenarios `CS-001` through `CS-015`.
- Support shared runner inputs for suite path, transport, auth mode, broker address, and output path.
- Emit normalized JSON with per-scenario verdicts and aggregate summary fields.
- Fail CI when any P0 scenario is not `pass`.

### 2. Connection, Auth, and Transport

- Rework `ConnectAsync` so auth success is protocol-driven instead of settle-delay-driven.
- Surface invalid JWT as `AuthenticationException`.
- Exercise both WebSocket and TCP in the conformance target.
- Cover anonymous, valid JWT, and invalid JWT flows in the conformance target.

### 3. Public Domain Surface Completion

- Expand RPC into a request/response API with worker registration and response handling semantics.
- Expand Queue into reserve/complete/extend/redelivery-capable APIs.
- Expand Notice into subscribe/unsubscribe and wildcard subscription APIs.
- Expand Schedule into list and notification subscription APIs.
- Expand Stream into APIs that can express subscription lifecycle, completion, and mid-flight errors.
- Expand KV operations where Fitz acceptance criteria require delete, insert protection, and scan semantics.

### 4. Timeout, Cancellation, Disconnect, and Reconnect

- Add end-to-end conformance tests for timeout and caller cancellation using public client APIs.
- Add disconnect-during-request coverage and explicit disconnect error mapping.
- Keep reconnect/backoff and state-rebuild semantics covered by unit and conformance tests.
- Validate shutdown during active work and double-close safety with conformance coverage.

### 5. Concurrency and Correlation

- Replace or constrain FIFO-by-message-type request matching.
- Validate same-domain concurrency with explicit correlation tests.
- Confirm chunked/multi-frame RPC and stream response semantics before finalizing the correlation design.

### 6. Auditability

- Keep the gap matrix current as parity work lands.
- Document the conformance command once the runner exists.
- Update examples only after the public surface is stable enough to avoid documenting temporary APIs.

## Evidence Sources

- `../fitz/docs/clients/client-acceptance-criteria.md`
- `../fitz/docs/clients/cross-language-conformance-suite.yaml`
- `../fitz/docs/clients/cross-language-conformance-runner.md`
- `src/Abstractions/**/*`
- `src/Core/**/*`
- `tests/Core/Unit/**/*`
- `tests/Core/Integration/**/*`
