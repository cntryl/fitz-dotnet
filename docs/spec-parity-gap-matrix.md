# Fitz-Dotnet Spec/Parity Gap Matrix

Date: 2026-04-24

Status legend:

- `implemented`: capability is represented and has direct evidence in the repo
- `partially implemented`: some core behavior exists, but the full contract is not covered
- `untested`: capability may exist, but there is no direct parity evidence
- `missing`: the public .NET surface cannot represent the contract behavior
- `contract-drift`: implementation exists, but behavior or artifact shape diverges from the shared Fitz contract

Current status: the shared conformance runner, transport matrix, and public domain surface are implemented; the remaining documented gaps are narrower protocol-shape items such as request correlation and error classification.

| Capability | .NET public surface | Acceptance criteria / scenarios | Current evidence | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Conformance runner | `tests/Core/Integration/ConformanceSmokeTests.Runner.cs`, `ConformanceSmokeTests.cs`, and `ConformanceModels.cs` | Runner contract, `CS-001` to `CS-015` | Shared runner parses the suite file, executes `CS-001` to `CS-015` in order, and writes normalized JSON output | `implemented` | CI fans out by transport and auth mode and uploads the conformance artifact |
| Transport matrix | `ClientConfig.Transport`, `TransportResolver.Resolve` | Suite required transports, `CS-001` to `CS-015` | WebSocket and TCP transports exist and are exercised in the CI spec matrix | `implemented` | Both transports are part of the shared conformance job |
| Connect and auth lifecycle | `IClient.ConnectAsync`, `Client`, `FitzConnection` | `AC-CONN-001` to `AC-CONN-005`, `CS-001`, `CS-002` | Connect performs an immediate auth probe and invalid JWT surfaces as `AuthenticationException` | `implemented` | Broker-backed tests cover anonymous and JWT auth across WebSocket and TCP |
| Reconnect contract | `ClientConfig`, `Client`, connection layer | `AC-CONN-006`, `CS-009`, `CS-010`, `CS-015` | Reconnect options exist; `FitzConnection` performs backoff reconnect and domain handlers rebuild connection-scoped state | `partially implemented` | End-to-end reconnect and shutdown proof is present, but this remains the most protocol-sensitive area |
| KV basic transaction lifecycle | `IKvClient`, `IKvTransaction` | `AC-KV-001` to `AC-KV-004`, `CS-003` | Begin/get/put/insert/delete/delete-range/scan/commit/rollback APIs exist and are unit-tested | `implemented` | The public surface now covers the KV contract that the old TODO called out |
| Queue producer/consumer lifecycle | `IQueueClient` | `AC-QUEUE-001` to `AC-QUEUE-008` | Public API exposes enqueue, reserve, subscribe, extend, complete, and completion-token handling | `implemented` | Queue operations are represented and covered by unit tests |
| Notice publish/subscribe | `INoticeClient` | `AC-NOTICE-001` to `AC-NOTICE-009` | Public API exposes publish and pattern subscriptions | `implemented` | Subscribe/unsubscribe and handler dispatch are present |
| RPC caller/worker lifecycle | `IRpcClient` | `AC-RPC-001` to `AC-RPC-008`, `CS-004`, `CS-007`, `CS-014` | Public API exposes streaming calls plus worker registration and response handling | `implemented` | The old “missing” surface has been replaced with a contract-shaped API |
| Lease lifecycle | `ILeaseClient`, `ILease` | `AC-LEASE-001` to `AC-LEASE-010` | Acquire/query plus extend/renew/release/subscribe APIs exist | `implemented` | Lease behavior is present and tested |
| Schedule lifecycle | `IScheduleClient` | `AC-SCHEDULE-001` to `AC-SCHEDULE-008` | Create/cancel/list/subscribe APIs exist | `implemented` | Scheduling now covers the listed contract operations |
| Stream ingest/read lifecycle | `IStreamClient`, `IStreamSession` | `AC-STREAM-001` to `AC-STREAM-005`, `AC-STREAM-010` to `AC-STREAM-014`, `CS-011` to `CS-013` | Begin/read/peek/metadata/subscribe APIs exist | `implemented` | The remaining design questions are about protocol nuances, not absence of surface |
| Timeout and cancellation | `ClientConfig.Timeout`, `FitzConnection.RequestAsync`, `Multiplexer.RequestAsync` | `CS-007`, `CS-008` | Unit tests and conformance scenarios prove timeout and cancellation behavior | `implemented` | The timeout/cancel behavior is already exercised in the shared suite |
| Disconnect and shutdown behavior | `Client.DisposeAsync`, `FitzConnection.CloseAsync` | `CS-009`, `CS-015` | Close cancels receive loop and pending requests; shutdown during reconnect backoff stays closed | `implemented` | The close/reconnect regression is covered and the client does not revive after shutdown |
| Response correlation and concurrency | `Multiplexer`, `RpcClient` | `CS-014`, `AC-RPC-002`, `AC-RPC-005` | Pending work is keyed by `messageType` FIFO; unit tests assert FIFO matching | `contract-drift` | Same-type concurrency depends on ordering, not explicit request identity |
| Error typing and retryability | Domain exceptions under `src/Core/Errors` | Error handling section, `CS-004` to `CS-008` | Domain exceptions expose `Code` and `Status`; no retryability classification | `contract-drift` | Auth and disconnect error mapping are still the main polish item |
| DI and service registration | `ServiceCollectionExtensions.AddFitzClient` | Operational parity support only | Basic singleton registration exists | `implemented` | Fine for current scope; not a parity blocker by itself |

## Notes

- `fitz-dotnet` now has the shared conformance runner, the transport/auth matrix in CI, and the full public domain surface represented in the abstractions layer.
- The remaining parity questions are narrower: request correlation/concurrency and a small amount of error-classification/reporting polish.
