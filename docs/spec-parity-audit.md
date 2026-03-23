# Fitz-Dotnet Spec/Parity Audit

Date: 2026-03-18
Audience: fitz-dotnet maintainers and Fitz client platform owners
Scope: internal parity audit against the shared Fitz client contract

## Summary

This audit evaluates `fitz-dotnet` against the shared Fitz client contract in `../fitz/docs/clients`, not just against local unit-test health.

Baseline facts used in this audit:

- `dotnet test tests/Core/Core.Tests.csproj --no-restore` passed with 31 tests on 2026-03-18.
- `fitz-dotnet` currently has integration smoke coverage for `CS-001` through `CS-004` only.
- The shared cross-language suite defines `CS-001` through `CS-015`.
- `fitz-go` and `fitz-py` already expose dedicated conformance targets aligned to the shared runner contract.
- `fitz-dotnet` currently supports WebSocket transport only.

Overall assessment:

- Core request/response plumbing exists for a subset of Fitz behavior.
- The repository is not yet spec-complete for Fitz client parity.
- The highest-risk gaps are public-surface incompleteness, auth-state correctness, conformance-runner drift, and missing TCP support.

## Findings

| Severity | Finding | Contracts affected | Recommended order |
| --- | --- | --- | --- |
| P0 | No contract-compliant .NET conformance runner exists yet | Runner contract, `CS-001` to `CS-015` | 1 |
| P0 | Connection auth is marked successful before broker confirmation and cannot reliably surface typed auth failure | `AC-CONN-002`, `AC-CONN-003`, `AC-CONN-005`, `CS-001`, `CS-002` | 2 |
| P0 | The current public domain surface does not represent several required Fitz capabilities | Required domains contract, `AC-QUEUE-*`, `AC-NOTICE-*`, `AC-RPC-*`, `AC-SCHEDULE-*`, `AC-STREAM-010` to `AC-STREAM-014` | 3 |
| P0 | TCP transport is required by the shared suite but unsupported in `fitz-dotnet` | Suite required transport matrix, `CS-001` to `CS-015` | 4 |
| P1 | Reconnect/backoff and connection-scoped state restoration are absent | `AC-CONN-006`, `CS-009`, `CS-010`, `CS-015` | 5 |
| P1 | In-flight response correlation depends on FIFO-by-message-type instead of explicit request identity | `CS-014`, `AC-RPC-002`, `AC-RPC-005` | 6 |
| P2 | Error/reporting shape and repo documentation lag behind parity needs | `CS-004` to `CS-008`, runner aggregate/result shape, release auditability | 7 |

### P0. No contract-compliant .NET conformance runner exists yet

Current evidence:

- `tests/Core/Integration/ConformanceSmokeTests.cs` executes only `CS-001` through `CS-004`.
- `tests/Core/Integration/ConformanceModels.cs` knows about `CS-001` through `CS-015`, but the executable suite does not cover them.
- The shared runner contract requires a dedicated runner that executes every scenario in order, emits normalized JSON, continues after failures, and exits non-zero when any P0 scenario is not `pass`.
- The current .NET aggregate shape hardcodes `client = fitz-dotnet`, `transport = websocket`, and `auth_mode = anonymous` instead of reporting actual run inputs.
- The current aggregate/result output does not match the runner contract fields such as `run_started_at`, `run_finished_at`, `summary`, and per-scenario `notes`.

Expected behavior:

- `fitz-dotnet` should expose a dedicated conformance target that can run the shared suite across supported transports and auth modes and emit machine-comparable results.

Likely root cause area:

- The repo has a smoke scaffold, not a finished conformance harness.

Recommended remediation:

1. Add a dedicated .NET conformance test target that executes `CS-001` through `CS-015`.
2. Accept runner inputs equivalent to the shared contract.
3. Emit the shared result shape instead of the current smoke-only aggregate.
4. Add CI gating for P0 scenarios.

### P0. Connection auth is marked successful before broker confirmation and cannot reliably surface typed auth failure

Current evidence:

- `src/Core/Connection/FitzConnection.cs` sends `CONNECT`, waits for a fixed settle delay, then unconditionally sets `State = Authenticated`.
- The same flow never parses an auth-success acknowledgment or a structured auth-failure frame.
- The receive loop converts transport failures into `Disconnected` state but does not translate auth rejection into `AuthenticationException`.
- `tests/Core/Integration/ConformanceSmokeTests.cs` currently treats a "silent-close model" for invalid JWT as a `partial` result path instead of a clear typed auth failure.

Expected behavior:

- `ConnectAsync` should not report success until the broker has accepted the session.
- Invalid JWTs should surface as authentication failures, not as delayed disconnects or ambiguous partial outcomes.
- Domain traffic should remain blocked until auth has definitively completed.

Likely root cause area:

- The connect state machine is delay-based rather than protocol-confirmation-based.

Recommended remediation:

1. Introduce explicit auth success/failure detection in the connection layer.
2. Map auth rejection to `AuthenticationException`.
3. Remove the fixed-delay success model as the source of truth.
4. Add end-to-end conformance coverage for anonymous, valid JWT, and invalid JWT auth flows.

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

### P0. TCP transport is required by the shared suite but unsupported in `fitz-dotnet`

Current evidence:

- `src/Core/Transport/TransportResolver.cs` supports only `"ws"` and `"websocket"` and throws `NotSupportedException` for other values.
- The shared suite marks both `websocket` and `tcp` as required transports.
- The shared runner guidance expects every client to be executable across both transports where the contract lists them.

Expected behavior:

- The client should either implement TCP transport parity or be explicitly treated as non-compliant until that support lands.

Likely root cause area:

- Transport work stopped after WebSocket bring-up.

Recommended remediation:

1. Add a TCP transport implementation and resolver path.
2. Extend the future conformance runner to exercise WebSocket and TCP.
3. Only claim parity once both transport legs are covered by tests.

### P1. Reconnect/backoff and connection-scoped state restoration are absent

Current evidence:

- `src/Core/ClientConfig.cs` has timeout and auth settings, but no reconnect/backoff configuration.
- `src/Core/Connection/FitzConnection.cs` exits the receive loop on disconnect and does not recreate the transport.
- The public API does not expose subscription or worker registration hooks that could be rebuilt after reconnect.
- The shared suite and contract require reconnect behavior, reconnect backoff, and rebuilding connection-scoped subscriptions/workers.

Expected behavior:

- When configured, the client should detect disconnects, reconnect, send `CONNECT` first on the new transport, and restore connection-scoped behavior.

Likely root cause area:

- The current client is a single-session transport wrapper, not a reconnect-capable session manager.

Recommended remediation:

1. Add reconnect/backoff options to `ClientConfig`.
2. Separate connection lifecycle from domain client instances so they can restore state.
3. Add conformance coverage for disconnect, reconnect, and shutdown-active-work scenarios.

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

- `ClientConfig`: likely needs reconnect/backoff controls, transport selection that can model TCP explicitly, and possibly knobs for reconnect restoration behavior.
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
- Add TCP transport support.
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
- Add reconnect/backoff options and state-rebuild semantics for connection-scoped behavior.
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
