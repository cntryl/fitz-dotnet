# fitz-dotnet

Current release: **4.2.0** (unreleased; see [CHANGELOG](CHANGELOG.md) for breaking changes), requiring Fitz **0.4.0 / Wire 3**. See [Wire 3 migration](docs/wire3-migration.md) for Queue TTL, NACK, negotiation, and coordinated upgrade guidance.

`fitz-dotnet` is the .NET Fitz client SDK. The repo now tracks the same shared 24-scenario conformance suite as `fitz-ts`, runs against both WebSocket and TCP, and includes a repo-owned broker baseline in [compose.yml](compose.yml).

## Packages

- `Cntryl.Fitz.Core`: core client API
- `Cntryl.Fitz.Abstractions`: public interfaces and shared contracts
- `Cntryl.Fitz.DependencyInjection`: DI registration helpers
- `Cntryl.Fitz.Extensions`: optional indexed KV-directory mechanics built on `Cntryl.LexKey`
- `Cntryl.Fitz.Testing`: deterministic in-memory KV transactions for consumer tests

`Cntryl.Fitz.Core` also ships the Roslyn analyzers and their code fixes, so installing
it is all a consumer does to get the compile-time route and lifetime diagnostics. They
run during your build and contribute nothing at runtime; there is no separate analyzer
package to reference and nothing to configure.

Every public type and member is documented, and the XML documentation ships in
each package, so IntelliSense works without consulting this file.

## Trimming and Native AOT

The runtime packages and optional testing package use no reflection on any code path — no `GetType()`,
no `Enum.IsDefined`, no container type activation — declare no
`DynamicallyAccessedMembers`/`RequiresUnreferencedCode`/`RequiresDynamicCode`
contracts, and suppress no trim or AOT warning. They are trim-safe and
Native-AOT-safe with no consumer-side configuration.

Enforcement is mechanical, not aspirational:

- every package sets `IsAotCompatible=true`, enabling the trim, AOT, and
  single-file analyzers, and warnings are errors repo-wide
- the Native AOT workflow publishes and runs a Native AOT executable against the
  freshly packed artifacts with **all five assemblies rooted**, so ILC analyzes
  every shipped method rather than only what the sample reaches, reports each
  finding individually, and fails the build on any of them

See [AOT and reflection](docs/aot-and-reflection.md) for the requirements and local verification commands.

Supplying your own transport through `ClientConfig.TransportFactory`? Override
`ITransport.TransportName` with a constant to label it in telemetry; it defaults
to `"custom"` rather than inspecting the runtime type.

## Install

```bash
dotnet add package Cntryl.Fitz.Core
dotnet add package Cntryl.Fitz.Abstractions
dotnet add package Cntryl.Fitz.DependencyInjection
dotnet add package Cntryl.Fitz.Extensions # optional; indexed KV directories
dotnet add package Cntryl.Fitz.Testing # optional; test projects only
```

The analyzers arrive with `Cntryl.Fitz.Core`. A discarded lease or an invalid route
literal becomes a build error, with a code fix offered in the IDE.

## One namespace

The production public surface lives in the single `Cntryl.Fitz` namespace, across all three
runtime packages, so a consumer writes one using directive rather than one per domain. The
assembly and package names keep their `.Abstractions` and `.DependencyInjection` suffixes;
`AddFitzClient` sits in `Cntryl.Fitz.DependencyInjection`, and the optional test doubles sit
in `Cntryl.Fitz.Testing`.

## Quick Start

```csharp
using Cntryl.Fitz;

await using var client = new FitzClient(
    new ClientConfig(
        new Uri("ws://127.0.0.1:4190/ws"),
        TokenProvider: _ => ValueTask.FromResult("your-jwt-token"),
        ServiceName: "orders-worker"
    )
);

await client.ConnectWhenReadyAsync();

var tx = await client.Kv.BeginAsync("kv://realm/app/users");
await tx.PutAsync("user-1"u8.ToArray(), """{"name":"Alice"}"""u8.ToArray());
await tx.CommitAsync(KvDurability.Sync);
```

`ClientConfig.ServiceName` is optional and is sent in the Wire 3 `CONNECT`
payload with the client build identity. Capability bits are reserved as zero in
Wire 3.

Generic Host consumers can register the same client through DI. The hosted
lifecycle connects during host startup and closes asynchronously during host
shutdown; consumers outside Generic Host can continue to call `ConnectAsync`
or `ConnectWhenReadyAsync` explicitly.

```csharp
services.AddFitzClient(new ClientConfig(
    new Uri("ws://127.0.0.1:4190/ws"),
    TokenProvider: _ => ValueTask.FromResult("your-jwt-token")));
```

## Testing KV consumers

`Cntryl.Fitz.Testing` supplies an `IKvClient` implementation without bringing in an
assertion framework or test host. It is route-isolated and transactional, clones all byte
buffers, supports read-your-writes, commit, rollback, read-only enforcement, optimistic
conflicts, bounded or unbounded mutation subscriptions, configurable scan page caps,
deterministic fault injection, reset-safe transactions, and operation history.

```csharp
using Cntryl.Fitz.Testing;

var kv = new InMemoryKvClient(new InMemoryKvClientOptions { ScanPageSize = 2 });
kv.Seed("kv://tenant/app/teams", "team/a"u8.ToArray(), payload);

var service = new TeamDirectory(kv);
var page = await service.ListAsync(/* ... */);

Assert.Contains(kv.Operations, operation => operation.Operation == KvTestOperation.Scan);
```

Production and test transactions can use `ScanAllAsync` to own continuation safely in
either direction; it rejects an impossible empty page whose `HasMore` flag is set. Fitz remains
key-schema neutral. Use `Cntryl.LexKey` to construct typed keys and range bounds, then pass
`LexKey.AsMemory()` directly to Fitz without copying.

## Indexed KV directories

`Cntryl.Fitz.Extensions` turns declared query shapes into bounded primary-key or covering-index
scans. The application owns which indexes exist and maps external sort or filter input to those
handles; Fitz owns atomic index maintenance, bounds, keyset continuation, and generation migration.

```csharp
var byName = new KvDirectoryIndex<Team>(
    "by_name", generation: 1,
    static team => [team.Name.ToUpperInvariant()]);

var teams = new KvDirectory<Team, Guid>(
    "teams",
    TeamJsonContext.Default.Team,
    static team => team.Id,
    static id => [id],
    [byName]);

await teams.InsertAsync(transaction, team, ct); // no read
var page = await teams.QueryAsync(client, route,
    byName.Query().Take(50).After(cursor), ct);

// Or through a transaction the caller already holds; a read-write one sees its staged writes.
var same = await teams.QueryAsync(transaction, byName.Query().Take(50).After(cursor), ct);

// Page the existing primary rows directly when identity order is the required order.
var primaryPage = await teams.QueryPrimaryAsync(transaction, limit: 50, cursor: primaryCursor, ct);
```

Record operations and queries accept a caller-owned `IKvTransaction`; the `(client, route)`
read forms are shorthand that open a short read-only transaction. Cursors are bound to the route
the query ran on, taken from `IKvTransaction.Route`, so a cursor from one route is rejected on
another.

Primary-key pages are ascending and bounded, and use their own route-bound opaque cursors.
They read the primary rows already maintained by the directory, including historical rows, so
they require no secondary index, backfill, migration marker, or request-path write.

`UpsertAsync` reads the previous primary record so it can remove old index rows. Hot paths that
already know the previous value use `ReplaceAsync(previous, current)` instead. To add an index
generation, deploy a schema containing both generations (ordinary writes then dual-write), backfill
the new generation in bounded batches, switch application reads, deploy a schema that no longer
writes the old generation, and finally delete the old range.

Runtime defaults now match the TS client truth surface:

- transport defaults to `auto`
- reconnect is enabled
- retry is enabled
- transport-native keepalive is enabled
- async handlers have no deadline unless `AsyncHandlerOptions.Timeout` is configured
- request queue size defaults to `1024`

KV commit, stream finalization, queue completion, and lease release become
terminal only after a successful response. A definite broker rejection leaves
the handle retryable; disposal remains bounded, best-effort cleanup.

## Subscription registrations

KV, Queue, Stream, Notice, RPC worker, and Schedule registrations accept exact
routes and whole-segment `*` or `**` patterns, including wildcard realms. KV,
Queue, and Stream patterns must be capable of matching three segments;
Schedule patterns must match four; Notice and RPC have flexible depth. The
broker permits 128 wildcard registrations per domain and session, while exact
registrations do not consume the quota. Lease subscriptions remain exact-only:
`lease://realm/area/resource`.

Queue reserve, Stream read, and Stream last responses must include the concrete
matched route for every item. Stream READ and SUBSCRIBE accept concrete resources,
`realm/area/*`, `realm/*/*`, or `stream://**`; Stream LAST is concrete-route only.
The client never substitutes a request pattern for a response route.

Notifications expose the exact concrete route. Queue availability events also
include ready, delayed, and inflight message counts. Active registrations are
restored after reconnect and duplicate local registrations share one wire
registration.

Queue reserves accept general whole-segment patterns capable of matching three
segments. Every returned `IQueueReservedItem` and `StreamReadItem`
exposes the concrete matched route, including `StreamRecord.Route` for event
records. Route-less reserve/read responses are not supported. If any item
contains an invalid concrete route, the entire response fails closed; the
client never returns a partial reservation or read batch.
Reserved queue items are async-disposable; dispose any item that will not be
completed so its connection-lifetime registration is released.

The queue `wait` duration uses the broker-native RESERVE wait field. A broker that
rejects that field fails the request directly; the client does not downgrade
to polling.

Every duration in the API is a `TimeSpan`, and one finer than the wire can carry is rejected
rather than silently rounded — see [docs/guide.md](docs/guide.md#durations).

`Stream.ReadAsync` follows validated continuation cursors until `HasMore` is
false. Use `ReadPageAsync` when the caller needs explicit page boundaries.

Subscriptions use `await client.Notice.SubscribeAsync(pattern)` (and the
equivalent domain method) and return typed handles implementing both
`IAsyncEnumerable<TNotification>` and `IAsyncDisposable`. Each local handle has
a bounded buffer; a slow consumer terminates with
`SubscriptionBackpressureException` without terminating sibling handles.
The capacity is configured by `AsyncHandlerOptions.SubscriptionBufferCapacity`
(default `256`), and a handle rejects concurrent enumeration explicitly.

Callback delivery uses an independent bounded queue configured with
`AsyncHandlerOptions.QueueCapacity` (default `1024`). Every subscription handle
exposes `Completion`: normal unsubscribe completes it, while callback queue
overflow faults it with `AsyncHandlerOverflowException`, terminates the local
registration, and is surfaced by async enumeration as the same typed failure.
Callbacks have no default deadline; configure `AsyncHandlerOptions.Timeout`
explicitly when the application wants slow callbacks to be cancelled.
An unsubscribe rejected by the broker remains retryable.
RPC worker saturation continues to use broker-visible protocol backpressure.
If the shared transport notification queue overflows, all subscription handles fault
with `SubscriptionBackpressureException` and the connection is reset. Create new
subscriptions after that terminal failure; reconnect cannot replay lost notifications.
For an A→B→C call, pass the handler's `RpcRequest.RemainingTime` and
`CancellationToken` to the downstream `CallAsync` so its budget and cancellation
are linked explicitly to the parent request.

Schedule broker saturation is `FitzErrorCodes.ScheduleBusy` (`7005`) and is
retryable when the wire effect is `NOT_APPLIED`. Backend errors use
`FitzErrorCodes.ScheduleBackendError` (`7006`) and keep the broker's reported
effect.

`Notice.UnsubscribeAllAsync()` sends broker message 503 and completes this
client's local subscriptions after acknowledgment. `Schedule.CreateBatchAsync()`
creates several schedules in one request, and `Schedule.ListAsync(continuation, limit)`
pages through schedules by cursor. RPC worker handler failures
send a terminal 6010 response to the caller and invoke the worker error hook.

## Local Verification

Fast local checks:

```bash
dotnet restore Fitz.slnx
dotnet build Fitz.slnx -c Release --no-restore
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build --filter "Category!=Integration"
```

Restore needs credentials for the private `cntryl` NuGet feed; see
[CONTRIBUTING.md](CONTRIBUTING.md#the-private-nuget-feed).

Verify the packed packages publish and run as a Native AOT executable with every
shipped assembly rooted (substitute your own runtime identifier):

```bash
dotnet pack Fitz.slnx -c Release --output artifacts/packages
rm -rf artifacts/consumer-package-cache
dotnet publish test/Fitz.PackageConsumer/Fitz.PackageConsumer.csproj \
  -c Release -r linux-x64 -p:PublishAot=true --output artifacts/native-aot
artifacts/native-aot/Fitz.PackageConsumer
```

Clearing `artifacts/consumer-package-cache` is required, not hygiene:
`PackageVersion` is fixed across rebuilds, so a copy left from an earlier pack
would shadow the packages just built and verify stale bits.

Broker-backed integration and conformance run:

```bash
docker compose up -d
.github/scripts/wait-for-broker.sh   # compose.yml disables healthchecks; this waits until the brokers answer
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build
docker compose down --volumes
```

Run a single conformance matrix leg and write the normalized artifact:

```bash
docker compose up -d
.github/scripts/wait-for-broker.sh
CONFORMANCE_TRANSPORT=websocket \
CONFORMANCE_AUTH_MODE=anonymous \
CONFORMANCE_OUTPUT=artifacts/conformance-results.json \
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build --filter "Category=Conformance"
docker compose down --volumes
```

The conformance artifact uses the shared schema:

- top-level `suite`, `version`, `generated_at`, `client`, `transport`, `auth_mode`, `p0_pass_rate`, `p1_pass_rate`, `overall_status`, and `scenarios`
- 17 scenarios from [conformance/cross-language-conformance-suite.yaml](conformance/cross-language-conformance-suite.yaml)
- one CI artifact per `websocket|tcp` x `anonymous|valid_jwt` leg
- broker-backed lifecycle coverage for KV, Queue, RPC, Lease, Notice, Stream, and Schedule

## Broker Baseline

[compose.yml](compose.yml) is the repo-owned local broker stack used by CI and local verification.

- both brokers use `ghcr.io/cntryl/fitz` pinned by digest (not a floating tag, so a broker release cannot change results on its own; Dependabot proposes bumps), local storage volumes, and loopback-only ports
- healthchecks are disabled, so `docker compose up -d` returns before the brokers listen; run `.github/scripts/wait-for-broker.sh` before testing
- `fitz-anon`: `ws://127.0.0.1:4190/ws` and `127.0.0.1:4191`
- `fitz-auth`: `ws://127.0.0.1:4090/ws` and `127.0.0.1:4091`
- default JWT secret: `dev-test-secret`
- default JWT audience: `fitz`

## Documentation

- [docs/guide.md](docs/guide.md) — the consumer guide: every domain, with working examples
- [docs/README.md](docs/README.md) — index of the user and engineering guides
- [CHANGELOG.md](CHANGELOG.md) — current release notes
- [CONTRIBUTING.md](CONTRIBUTING.md) — enforced standards, and where the normative client
  protocol specification lives (the Fitz server repository, not this one)
- [docs/aot-and-reflection.md](docs/aot-and-reflection.md) — the trim and AOT guarantee
- [performance guidance](docs/performance.md) — performance guidance

## Managed leases

`ILeaseClient.WithLeaseAsync` owns acquisition, renewal, callback cancellation, and release
for `ValueTask` callbacks. Set `LeaseExecutionOptions.WaitForAvailability` to retry typed
contention. Callback code must honor its cancellation token promptly. Low-level handles
remain available, serialize fencing-token rotation, and close on uncertain renewal.
Low-level `ILease` handles expose their current read-only `FencingToken`.
The callback token is canceled immediately when the client observes a disconnect or other
lease-ownership loss. Caller cancellation remains `OperationCanceledException`, and a cleanup
failure never replaces the callback or lease-loss exception already leaving the scope.

Authority-aware callbacks also receive the immutable admission fence from the successful
`ACQUIRE` response:

```csharp
await client.Lease.WithLeaseAsync(
    "lease://example/jobs/leader",
    TimeSpan.FromSeconds(30),
    async (authority, ct) =>
    {
        await RunLeaderAsync(authority.FencingToken, ct);
    });
```

`LeaseAuthority.FencingToken` is the admission epoch for that callback, not a live renewal
credential. It remains stable while the managed lease renews. Tokens are ordered only across
successive owners of the same lease route; an external store should atomically retain the greatest
accepted value and reject lower values rather than compare the snapshot for equality with a later
broker token. Existing one-argument callbacks remain supported.

## Repository Layout

- `src/Fitz.Core/Fitz.Core.csproj`: core SDK package and client runtime
- `src/Fitz.Abstractions/Fitz.Abstractions.csproj`: shared interfaces and contracts
- `src/Fitz.DependencyInjection/Fitz.DependencyInjection.csproj`: DI registration extensions
- `test/Fitz.Core.Tests/Fitz.Core.Tests.csproj`: unit, integration, and shared-suite conformance coverage


### Queue backpressure and acknowledgement retries

Queue capacity error `4005` means the request was rejected before admission.
Use bounded backoff and a cancellation or deadline budget. For a rejected ACK,
keep the existing reservation and retry its completion while the lease remains
valid; do not rerun the work handler just to retry the ACK. Successful completion
closes the reservation.

A timeout, disconnect, cancellation after sending, or error `4007` can leave the
outcome unknown. Do not blindly replay enqueue, reserve, or completion in those
cases. Reconnection invalidates old reservation handles. A retryable error is a
classification, not a guarantee of automatic retries or exactly-once processing.

### Commit-time persistence

KV BEGIN selects only scope and transaction mode. Choose Buffered or Sync explicitly at each KV and Stream COMMIT. Buffered maps to local Buffered or cloud CloudAsync; Sync maps to local Sync or cloud CloudStrict and is never silently downgraded. Uncommitted transaction contexts roll back. Queue remains best effort.

`KvDurability.Async` is renamed to `KvDurability.Buffered`. `KvDirectory.BackfillAsync` also requires a durability argument because it commits each batch.
