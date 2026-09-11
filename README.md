# fitz-dotnet

`fitz-dotnet` is the .NET Fitz client SDK. The repo now tracks the same shared 17-scenario conformance suite as `fitz-ts`, runs against both WebSocket and TCP, and includes a repo-owned broker baseline in [compose.yml](compose.yml).

## Packages

- `Cntryl.Fitz`: core client API
- `Cntryl.Fitz.Abstractions`: public interfaces and shared contracts
- `Cntryl.Fitz.DependencyInjection`: DI registration helpers

All three packages support trimming and Native AOT. They enable the .NET AOT
compatibility analyzers, and CI publishes and executes a Native AOT application
against the packed NuGet artifacts.

## Install

```bash
dotnet add package Cntryl.Fitz
dotnet add package Cntryl.Fitz.Abstractions
dotnet add package Cntryl.Fitz.DependencyInjection
```

## Quick Start

```csharp
using Cntryl.Fitz;

await using var client = new Client(
    new ClientConfig(
        new Uri("ws://127.0.0.1:4190/ws"),
        TokenProvider: _ => ValueTask.FromResult("your-jwt-token")
    )
);

await client.ConnectWhenReadyAsync();

var tx = await client.Kv.BeginAsync("kv://realm/app/users", Cntryl.Fitz.Abstractions.Domains.Kv.KvDurability.Async);
await tx.PutAsync("user-1"u8.ToArray(), """{"name":"Alice"}"""u8.ToArray());
await tx.CommitAsync();
```

Generic Host consumers can register the same client through DI. The hosted
lifecycle connects during host startup and closes asynchronously during host
shutdown; consumers outside Generic Host can continue to call `ConnectAsync`
or `ConnectWhenReadyAsync` explicitly.

```csharp
services.AddFitzClient(new ClientConfig(
    new Uri("ws://127.0.0.1:4190/ws"),
    TokenProvider: _ => ValueTask.FromResult("your-jwt-token")));
```

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

Queue `waitSeconds` uses the broker-native RESERVE wait field. A broker that
rejects that field fails the request directly; the client does not downgrade
to polling.

The current queue wire does not include an attempt count, so
`QueueItem.Attempt` is `QueueItem.AttemptUnavailable` (`0`). Queue delays are
accepted only in whole-second `delayMs` values instead of being silently rounded.

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

Schedule backend unavailability and broker saturation use the distinct coded
error `FitzErrorCodes.ScheduleBackendError` (`7010`). `Retryability` classifies
it as retryable subject to operation safety; it is not reported as malformed
cron or parse input.

## Unreleased preview migration

This preview intentionally breaks the earlier callback subscription surface.
Replace callback arguments with `await foreach` over the returned handle. Public
one-shot operations now return `Task`/`Task<T>`; `ValueTask` remains only for
disposal and callback/provider contracts. Schedule listing now uses
`ListAsync(offset, limit)` and returns entries plus `TotalCount`.

## Local Verification

Fast local checks:

```bash
dotnet restore Fitz.sln
dotnet build Fitz.sln -c Release --no-restore
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build --filter "FullyQualifiedName!~Integration"
```

Broker-backed integration and conformance run:

```bash
docker compose up -d
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build
docker compose down --volumes
```

Run a single conformance matrix leg and write the normalized artifact:

```bash
docker compose up -d
CONFORMANCE_TRANSPORT=websocket \
CONFORMANCE_AUTH_MODE=anonymous \
CONFORMANCE_OUTPUT=artifacts/conformance-results.json \
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build --filter FullyQualifiedName~Conformance
docker compose down --volumes
```

The conformance artifact uses the shared schema:

- top-level `suite`, `version`, `generated_at`, `client`, `transport`, `auth_mode`, `p0_pass_rate`, `p1_pass_rate`, `overall_status`, and `scenarios`
- 17 scenarios from [conformance/cross-language-conformance-suite.yaml](conformance/cross-language-conformance-suite.yaml)
- one CI artifact per `websocket|tcp` x `anonymous|valid_jwt` leg
- broker-backed lifecycle coverage for KV, Queue, RPC, Lease, Notice, Stream, and Schedule

## Broker Baseline

[compose.yml](compose.yml) is the repo-owned local broker stack used by CI and local verification.

- both brokers use `ghcr.io/cntryl/fitz:latest`, local storage volumes, and loopback-only ports
- `fitz-anon`: `ws://127.0.0.1:4190/ws` and `127.0.0.1:4191`
- `fitz-auth`: `ws://127.0.0.1:4090/ws` and `127.0.0.1:4091`
- default JWT secret: `dev-test-secret`
- default JWT audience: `fitz`

## Documentation

- [docs/README.md](docs/README.md)
- [CLIENT_SPEC.md](CLIENT_SPEC.md)
- [CLIENT_ACCEPTANCE_CRITERIA.md](CLIENT_ACCEPTANCE_CRITERIA.md)
- [docs/spec-parity-gap-matrix.md](docs/spec-parity-gap-matrix.md)
- [docs/spec-parity-audit.md](docs/spec-parity-audit.md)

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
    30,
    async (authority, cancellationToken) =>
    {
        await RunLeaderAsync(authority.FencingToken, cancellationToken);
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
