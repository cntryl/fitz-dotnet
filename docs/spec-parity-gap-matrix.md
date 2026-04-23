# Fitz-Dotnet Spec/Parity Gap Matrix

Date: 2026-03-18

Status legend:

- `implemented`: capability is represented and has direct evidence in the repo
- `partially implemented`: some core behavior exists, but the full contract is not covered
- `untested`: capability may exist, but there is no direct parity evidence
- `missing`: the public .NET surface cannot represent the contract behavior
- `contract-drift`: implementation exists, but behavior or artifact shape diverges from the shared Fitz contract

| Capability | .NET public surface | Acceptance criteria / scenarios | Current evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Conformance runner | `tests/Core/Integration/ConformanceSmokeTests.Runner.cs`, `ConformanceSmokeTests.cs`, and `ConformanceModels.cs` | Runner contract, `CS-001` to `CS-015` | Shared runner parses the suite file, executes `CS-001` to `CS-015` in order, and writes normalized JSON output | `implemented` | Matrix fan-out / artifact retention still needs CI wiring |
| Transport matrix | `ClientConfig.Transport`, `TransportResolver.Resolve` | Suite required transports, `CS-001` to `CS-015` | WebSocket and TCP transports exist; the shared runner still needs coverage across both | `implemented` | Conformance coverage is still websocket-heavy |
| Connect and auth lifecycle | `IClient.ConnectAsync`, `Client`, `FitzConnection` | `AC-CONN-001` to `AC-CONN-005`, `CS-001`, `CS-002` | Connect now performs an immediate auth probe and invalid JWT surfaces as `AuthenticationException` | `partially implemented` | Broker-backed proof across WebSocket and TCP is still needed |
| Reconnect contract | `ClientConfig`, `Client`, connection layer | `AC-CONN-006`, `CS-009`, `CS-010`, `CS-015` | Reconnect options exist; `FitzConnection` performs backoff reconnect and domain handlers rebuild connection-scoped state | `partially implemented` | Contract-level reconnect and shutdown coverage is still thinner than the implementation |
| KV basic transaction lifecycle | `IKvClient`, `IKvTransaction` | `AC-KV-001` to `AC-KV-004`, `CS-003` | Begin/get/put/commit/rollback APIs exist; smoke test covers basic write/read path | `partially implemented` | Delete, insert protection, and scan criteria are absent from public API |
| Queue producer/consumer lifecycle | `IQueueClient` | `AC-QUEUE-001` to `AC-QUEUE-008` | Public API exposes enqueue only | `missing` | Reserve, complete, extend, and redelivery are not representable |
| Notice publish/subscribe | `INoticeClient` | `AC-NOTICE-001` to `AC-NOTICE-009` | Public API exposes publish only | `missing` | Subscribe, wildcard matching, unsubscribe, and multiplexing are absent |
| RPC caller/worker lifecycle | `IRpcClient` | `AC-RPC-001` to `AC-RPC-008`, `CS-004`, `CS-007`, `CS-014` | Caller API returns `Task`, not response payload; no worker registration API; correlation ID generated client-side but not used in dispatch | `missing` | Current API cannot express Fitz RPC parity |
| Lease lifecycle | `ILeaseClient`, `ILease` | `AC-LEASE-001` to `AC-LEASE-010` | Acquire/query plus extend/renew/release APIs exist | `partially implemented` | Acceptance-criteria breadth exceeds current evidence and tests |
| Schedule lifecycle | `IScheduleClient` | `AC-SCHEDULE-001` to `AC-SCHEDULE-008` | Create/cancel APIs exist | `partially implemented` | List and schedule notification subscription semantics are absent |
| Stream ingest/read lifecycle | `IStreamClient`, `IStreamSession` | `AC-STREAM-001` to `AC-STREAM-005`, `AC-STREAM-010` to `AC-STREAM-014`, `CS-011` to `CS-013` | Begin/append/commit/rollback/read/metadata APIs exist; read path is one request returning buffered records | `contract-drift` | No subscription lifecycle, completion signal, or mid-flight error surface |
| Timeout and cancellation | `ClientConfig.Timeout`, `FitzConnection.RequestAsync`, `Multiplexer.RequestAsync` | `CS-007`, `CS-008` | Unit tests prove timeout and cancellation inside the multiplexer | `partially implemented` | End-to-end parity evidence and post-timeout/post-cancel health checks are missing |
| Disconnect and shutdown behavior | `Client.DisposeAsync`, `FitzConnection.CloseAsync` | `CS-009`, `CS-015` | Connection close cancels receive loop and pending requests; close during reconnect backoff no longer revives the client | `partially implemented` | Unit coverage now includes reconnect-backoff shutdown, but the shared contract runner still needs broader active-work scenarios |
| Response correlation and concurrency | `Multiplexer`, `RpcClient` | `CS-014`, `AC-RPC-002`, `AC-RPC-005` | Pending work is keyed by `messageType` FIFO; unit tests assert FIFO matching | `contract-drift` | Same-type concurrency depends on ordering, not explicit request identity |
| Error typing and retryability | Domain exceptions under `src/Core/Errors` | Error handling section, `CS-004` to `CS-008` | Domain exceptions expose `Code` and `Status`; no retryability classification | `contract-drift` | Auth and disconnect error mapping are especially incomplete |
| DI and service registration | `ServiceCollectionExtensions.AddFitzClient` | Operational parity support only | Basic singleton registration exists | `implemented` | Fine for current scope; not a parity blocker by itself |

## Notes

- `fitz-dotnet` is strongest today in basic WebSocket connection setup, KV happy-path operations, and low-level timeout/cancellation primitives.
- The largest parity blockers are the incomplete domain abstractions, auth-state correctness, incomplete TCP conformance coverage, and the absence of a contract-compliant conformance runner.
