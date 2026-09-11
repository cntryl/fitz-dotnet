# Fitz .NET sharp-edges evidence ledger

This ledger is the disposition record for the sharp-edges audit. The audit summary called out 62 findings, but its enumerated identifiers contain 80 items: 14 connection, 8 multiplexer, 8 buffer, 6 transport, 22 API, 12 subscription, and 10 package findings. All 80 identifiers are retained here so none disappear because of the source count mismatch.

Statuses have these meanings:

- **confirmed**: the finding reproduced or was evident in the implementation and has been remediated in this change.
- **rejected**: executable or policy evidence contradicts the finding.
- **intentionally accepted**: the tradeoff remains by design and is documented here.
- **protocol-deferred**: a safe client-only remediation is impossible without a Fitz wire-protocol change.

## Phase 1: correctness

| ID | Status | Disposition and evidence |
|---|---|---|
| CONN-1 | confirmed | Request failures now schedule connection recovery in the background and return the original timeout, cancellation, or transport failure immediately. Covered by `ClientTests.ShouldSurfaceRequestFailureWithoutWaitingForBackgroundReconnect`. |
| CONN-2 | protocol-deferred | The current protocol has no positive authentication acknowledgement. The client retains the compatibility settlement window and treats an explicit rejection as authoritative. |
| CONN-3 | protocol-deferred | There is no Fitz application heartbeat frame. The unused heartbeat timer and activity timestamps were removed; TCP keepalive remains enabled and WebSocket keepalive configures native PING/PONG timeout detection from `HeartbeatOptions.Timeout`. |
| CONN-4 | rejected | No client behavior consumes inbound or outbound activity timestamps, so recording them provided no liveness guarantee. The unused timestamps and write-only markers were removed. |
| CONN-5 | confirmed | Connect and reconnect cancellation ownership is internal, atomically replaced, and disposed only after the owning operation completes. |
| CONN-6 | confirmed | Reconnect listeners are snapshotted and all invoked independently. Their failures are logged, aggregated, and propagated so a partially restored session is failed and retried rather than declared healthy. |
| CONN-7 | confirmed | `Client.CloseAsync` now disposes an initialized RPC client with the other domain clients. |
| CONN-8 | confirmed | Domain clients use `Lazy<T>` with execution-and-publication thread safety. |
| CONN-9 | confirmed | A shared connect operation uses an internal token; each caller applies its own cancellation only while waiting for that operation. |
| CONN-10 | confirmed | Lifecycle state and transport replacement use lock, volatile, and interlocked synchronization consistently. |
| CONN-11 | confirmed | Connection-closed token sources are retired and disposed after readers can no longer race replacement. |
| CONN-12 | confirmed | All request retries and backoff share one operation deadline and use jittered exponential delays. Caller cancellation remains `OperationCanceledException`. Covered by the retry deadline and cancellation tests in `ClientTests`. |
| CONN-13 | confirmed | Startup readiness retries only transient connection, timeout, I/O, socket, and WebSocket failures. Configuration is validated before the first attempt. |
| CONN-14 | confirmed | Lifecycle logging, custom tracing, custom metrics, and standard diagnostics are guarded so instrumentation cannot corrupt connection state. |
| MUX-1 | protocol-deferred | Most Fitz request/response messages carry no correlation identifier. The client must preserve one serialized lane per uncorrelated response message type. |
| MUX-2 | confirmed | Unsafe optional-response timeout compensation was removed. A sent uncorrelated request that times out desynchronizes the session and fails all affected pending requests. |
| MUX-3 | confirmed | When correlated requests exist, a matcher-rejected response cannot fall through to an uncorrelated waiter or unrelated request. Covered by multiplexer response-correlation tests. |
| MUX-4 | confirmed | Notification callbacks run on a bounded asynchronous pump rather than inline on the transport receive loop. |
| MUX-5 | confirmed | Notification callback failures and dispatch overflow are isolated and reported through the configured error sink. |
| MUX-6 | confirmed | Request lanes are not disposed while concurrent waiters can still release or observe them; connection resets cancel a replaceable lane-state token. |
| MUX-7 | confirmed | Multiplexer request and notification scans use allocation-free loops rather than LINQ. Notification fan-out arrays are cached per message type and rebuilt only on registration change, and one queue entry is enqueued per frame rather than per subscriber, so receive-loop dispatch cost no longer scales with subscriber count. RPC worker route selection remains a separate O(n) pattern scan with per-candidate parsing and is not covered by this disposition. |
| MUX-8 | confirmed | Message type zero no longer acts as an internal sentinel. |
| BUF-1 | confirmed | The 65,535-byte payload ceiling is an explicit Fitz wire limit and violations now raise `ProtocolException`. Both TCP and WebSocket reject outbound frames above the configured `MaxFrameSize`. |
| BUF-2 | confirmed | Every pooled `WrittenMemory` escape is awaited while its writer remains alive. Regression coverage holds notice, stream commit, and stream rollback sends open until the asynchronous consumer finishes. |
| BUF-3 | confirmed | Readers and frame parsers validate declared lengths and checked growth before allocation. The parser checks its configured cap before accepting bytes, including caps below its normal initial capacity and frames fragmented across transport messages. |
| BUF-4 | confirmed | `BinaryBufferWriter` and `PooledFrame` reject access after disposal. |
| BUF-5 | confirmed | Pooled writer, frame, TCP, and WebSocket buffers are returned with their written region zeroed; the untouched tail of a rent is left alone, so clearing cost tracks frame size rather than pool bucket size. Authentication token and encoded connect bytes are zeroed explicitly. Covered by the pooled-buffer clearing tests in `HotPathRegressionTests`. |
| BUF-6 | intentionally accepted | Request/response decoding raises typed `ProtocolException` failures for malformed and truncated wire data. Uncorrelated malformed notifications are dropped at receive-loop isolation boundaries and currently have no public failure channel. |
| BUF-7 | intentionally accepted | The internal borrowed-notification registration names a lifetime boundary, but the multiplexer currently materializes owned arrays before dispatch, so it provides no receive-path allocation reduction today. Asynchronous/public boundaries, including deferred lease grants, explicitly receive owned copies. |
| BUF-8 | intentionally accepted | `TryReadFrame` intentionally returns a borrowed parser view while `ParseFrames` returns owned frames. XML documentation makes the ownership distinction explicit without a compatibility-breaking API replacement. |
| TR-1 | protocol-deferred | The Fitz TCP URI scheme has no TLS negotiation or certificate contract. TLS-over-TCP requires a protocol/endpoint design; secure WebSocket remains available. |
| TR-2 | confirmed | TCP keepalive enables supported idle time, interval, and retry-count socket options rather than only toggling the boolean. |
| TR-3 | confirmed | TCP and WebSocket sends acquire their serialization gate before reading the replaceable transport object. Replacement uses interlocked ownership. |
| TR-4 | confirmed | Send cancellation caused by the caller remains `OperationCanceledException`; transport timeout translation is limited to an elapsed transport deadline. |
| TR-5 | intentionally accepted | The public WebSocket option surface stays narrow. Advanced platform customization remains available through `ClientConfig.TransportFactory`, avoiding a larger public API. |
| TR-6 | confirmed | TCP framing is assembled into one pooled contiguous buffer and written with one asynchronous stream call. |
| API-1 | protocol-deferred | The global lease-acquisition gate is retained deliberately: deferred lease grants contain no route or request identifier. Removing it could grant a lease to the wrong caller. |
| API-2 | protocol-deferred | Deferred lease grants are positional on the current wire. Tests prove concurrent acquisitions cannot cross callers while the safety gate is present. |
| API-3 | confirmed | `ILease.FencingToken` is the live credential and updates after successful renewal. `WithLeaseAsync` keeps its admission authority stable but now detects token rotation, cancels the callback lifecycle, and surfaces typed lease loss instead of silently leaving the callback with stale authority. |
| API-4 | confirmed | Renewal uses the lease lifecycle token plus a bounded per-operation deadline. The deadline has a two-second floor where remaining lease time permits, while staying below the lease time remaining and the five-second ceiling. |
| API-5 | confirmed | `WithLeaseAsync` preserves caller cancellation as `OperationCanceledException`, prioritizes the primary callback or lease-loss failure, and suppresses secondary cleanup failures. |
| API-6 | confirmed | Queue attempt count is no longer fabricated as one. `QueueItem.AttemptUnavailable` is the documented zero sentinel until the wire carries attempt metadata. |
| API-7 | confirmed | Reserved queue items are async-disposable. Successful completion and explicit disposal both close the local handle and release its disconnect listener; the wire still has no nack operation. |
| API-8 | protocol-deferred | The current RPC framing has no interoperable terminal-error response for arbitrary worker exceptions. The client isolates handler failure, but cannot reliably terminate the remote caller without a protocol addition. |
| API-9 | confirmed | RPC responses use one notification registration and an O(1) correlation-ID dictionary instead of one handler and a scan per call. |
| API-10 | confirmed | Transaction, stream, lease, subscription, and worker disposal network operations are bounded and best-effort so cleanup cannot replace an exception already leaving a scope. Handle disposal closes local state and releases disconnect listeners even when broker cleanup fails. |
| API-11 | confirmed | Queue completion, transaction commit/rollback, stream commit/rollback, lease release, subscription unsubscribe, and worker unregister transition terminal state only after a successful wire result; ambiguous failures remain retryable. |
| API-12 | confirmed | Retryability is based on typed exceptions and domain codes; English exception-message matching was removed. |
| API-13 | confirmed | `Stream.ReadAsync` continues through validated cursors until `HasMore` is false while preserving cancellation, fingerprint, and watermark state. |
| API-14 | confirmed | Queue delays must be non-negative whole seconds; the client no longer silently rounds values, and FITZ004 diagnoses invalid compile-time constants. |
| API-15 | intentionally accepted | Queue reserve response decoding remains selected by the concrete request mode, as defined by the existing wire contract. Adding a response discriminator would be a protocol change. |
| API-16 | protocol-deferred | The current queue protocol has no nack/failed-batch command or attempt metadata. The client exposes the unavailable-attempt sentinel and cannot safely invent either operation. |
| API-17 | confirmed | `RpcClient.CallAsync` is a non-iterator validation wrapper, so invalid routes and missing streaming configuration fail at the call site. |
| API-18 | confirmed | RPC workers enforce configured concurrency, return bounded backpressure responses, prefer exact routes, and resolve wildcard ties deterministically by specificity and ordinal pattern. |
| API-19 | confirmed | RPC caller sequence, terminal-error, and channel-completion transitions are correlated and serialized. Worker response writers also serialize concurrent sends, assign sequence numbers in send order, and reject sends after a successful terminal frame. |
| API-20 | confirmed | Connection-loss paths now use neutral closed-or-reset failures instead of asserting that every closure was a remote disconnect. |
| API-21 | intentionally accepted | Existing public domain exception/code mapping remains stable where the wire does not expose a more precise route failure. Changing those contracts would be a compatibility break without additional wire evidence. |
| API-22 | intentionally accepted | Queue, KV, lease, stream, schedule, and most RPC decoders validate counts, lengths, flags, and trailing bytes. RPC worker-subscription success decoding remains permissive and discards its opaque response payload for compatibility with existing broker variants. |
| SUB-1 | confirmed | Each domain subscription table uses one gate for read, mutation, unsubscribe, and restore transitions. Client disposal does not dispose gates underneath active holders or waiters. |
| SUB-2 | confirmed | Notification-handler registration initialization is synchronized and creates at most one registration per domain/message type. |
| SUB-3 | confirmed | Async handler concurrency is bounded by default to at least one and at most the configured concurrency, with a bounded queue. |
| SUB-4 | confirmed | Subscription pump/callback failures fault only the affected handle and are surfaced through `Completion`; they are no longer silently swallowed. |
| SUB-5 | confirmed | Per-subscription buffering uses configurable bounded channels and overflow terminates the affected subscription with a typed backpressure failure. The separate multiplexer notification and reconnect buffers remain fixed at 1,024 entries. |
| SUB-6 | confirmed | Callback and async-enumerable subscription paths derive their limits from the same `AsyncHandlerOptions` capacity. |
| SUB-7 | confirmed | A generic subscription explicitly rejects concurrent enumeration rather than splitting one channel nondeterministically between consumers. |
| SUB-8 | confirmed | Explicit unsubscribe becomes terminal only after broker success and can be retried after an ambiguous failure. |
| SUB-9 | confirmed | User callbacks and diagnostic hooks execute outside subscription and dispatcher locks. |
| SUB-10 | intentionally accepted | Async-handler dispatch is bounded per domain, not client-wide: each lazily created domain dispatcher has its own configured concurrency and queue capacity. Per-subscription buffers isolate stream delivery and identify the subscription that overflowed. |
| SUB-11 | confirmed | Subscription cancellation sources remain alive until their pump exits, eliminating dispose-versus-linked-token races. |
| SUB-12 | confirmed | Reconnect restore builds replacement registrations first and swaps subscription maps atomically from the dispatcher's perspective. A partial restore best-effort unsubscribes every replacement already created before preserving the original failure. |
| SUB-13 | confirmed | Lease inventory bootstrap and steady-state convergence cap immediate full-LIST retries. Normal subscription completion stops reconciliation and completes `Updates` instead of launching an unbounded recovery loop after client shutdown. |

## Phase 2: ergonomics

| ID | Status | Disposition and evidence |
|---|---|---|
| PKG-1 | rejected | `AssemblyVersion` intentionally remains `1.0.0.0` for binary compatibility while `PackageVersion` advances independently. The packed Native AOT consumer is the executable compatibility proof. |
| PKG-2 | confirmed | `AddFitzClient` registers a hosted lifecycle that connects during host startup and closes asynchronously during shutdown. Covered by `DependencyInjectionTests`. Explicit `ConnectAsync` remains available outside Generic Host. |
| PKG-3 | confirmed | `Client` now implements synchronous `IDisposable` in addition to `IAsyncDisposable`, so a synchronously disposed service provider initiates safe asynchronous cleanup without sync-over-async. |
| PKG-4 | intentionally accepted | `ClientConfig` remains the explicit composition object. Additional convenience overloads were not added because they would expand public API without improving lifecycle safety. |
| PKG-5 | confirmed | Standard `ActivitySource` and `Meter` instruments now bridge connection lifecycle, retry, timeout, queue pressure, and handler saturation while preserving guarded custom hooks. |
| PKG-6 | confirmed | Constant-route analyzer diagnostics are warnings by default and remain opt-in build errors through standard analyzer configuration. Runtime validation still covers dynamic routes. Analyzer rules resolve the actual Fitz interface symbols, accept stream read selectors, and are built against the minimum supported Roslyn 5.0 API surface. |
| PKG-7 | confirmed | Resolved default option objects are cached rather than allocated on every access. |
| PKG-8 | intentionally accepted | Default interface members remain for binary/source compatibility; replacing them with abstract members would break existing consumers. |
| PKG-9 | intentionally accepted | Package metadata includes descriptions, author, license, repository, symbols where applicable, deterministic debug metadata, and SourceLink. XML documentation files are not currently generated or shipped; enabling them cleanly requires completing the public API documentation surface rather than suppressing missing-comment diagnostics. |
| PKG-10 | intentionally accepted | Public option records retain record equality and delegate-bearing shape for compatibility. Central constructor validation now rejects invalid configuration before connection work starts. |

## Phase 3: performance and resource bounds

Performance findings from the audit are represented above at `MUX-4`, `MUX-7`, `BUF-2`, `BUF-3`, `BUF-5`, `TR-6`, `API-9`, `SUB-3`, `SUB-5`, `SUB-6`, `SUB-10`, and `PKG-7`. The implementation uses bounded queues and channels, validates allocation sizes before renting or allocating, keeps pooled storage alive across asynchronous sends, clears sensitive/consumer data on return, and removes per-call RPC response-handler scans. The remaining RPC worker pattern scan is explicitly recorded under `MUX-7`. Benchmark discovery and smoke execution are part of the verification record below.

## Verification record

- `dotnet build Fitz.sln -c Release --no-restore`: passed with zero warnings and zero errors.
- Release validation passed 369 non-integration core tests and 14 analyzer tests.
- The 30 broker-backed integration tests passed over WebSocket and again over TCP, including forced broker restart and same-client recovery.
- `dotnet format Fitz.sln --no-restore --verify-no-changes --severity warn`: passed; new behavioral tests use the `ShouldXGivenYWhenZ` convention.
- `dotnet pack Fitz.sln -c Release --no-restore --output artifacts/packages`: passed for all packable projects. Runtime packages include symbols and SourceLink; Roslyn-only packages explicitly skip empty symbol packages.
- A clean NuGet-cache restore and execution of `Fitz.PackageConsumer` passed from the packed `0.1.3` artifacts and verified all three runtime assembly versions are `1.0.0.0`; an exact packed-consumer Native AOT publish and executable run also passed.
- The packed analyzer loaded and compiled valid stream-selector usage with SDK `10.0.109` / Roslyn 5.0 without CS9057.
- Benchmark discovery found 24 benchmark methods. `MultiplexerHotPathBenchmarks.RequestDispatchRoundTrip` completed at a 2.098 microsecond mean with 1.03 KB allocated, below the documented 5 microsecond dispatch target.

## Protocol follow-up requirements

The protocol-deferred items reduce to seven wire additions: request-wide correlation identifiers, a positive authentication acknowledgement, an application heartbeat frame, TLS negotiation for TCP endpoints, correlated deferred lease grants, queue nack/attempt metadata, and RPC terminal-error framing for worker failures. These require coordinated broker and cross-client changes; none can be safely emulated by this client alone.
