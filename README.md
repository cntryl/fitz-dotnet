# fitz-dotnet

`fitz-dotnet` is the .NET Fitz client SDK. The repo now tracks the same shared 17-scenario conformance suite as `fitz-ts`, runs against both WebSocket and TCP, and includes a repo-owned broker baseline in [compose.yml](compose.yml).

## Packages

- `Cntryl.Fitz`: core client API
- `Cntryl.Fitz.Abstractions`: public interfaces and shared contracts
- `Cntryl.Fitz.DependencyInjection`: DI registration helpers

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

var tx = await client.Kv().BeginAsync("kv://realm/app/users");
await tx.PutAsync("user-1"u8.ToArray(), """{"name":"Alice"}"""u8.ToArray());
await tx.CommitAsync();
```

Runtime defaults now match the TS client truth surface:

- transport defaults to `auto`
- reconnect is enabled
- retry is enabled
- heartbeat is enabled
- async handler timeout defaults to the client timeout
- request queue size defaults to `1024`

## Subscription registrations

KV, Queue, Stream, Notice, RPC worker, and Schedule registrations accept exact
routes and whole-segment `*` or `**` patterns, including wildcard realms. KV,
Queue, and Stream patterns must be capable of matching three segments;
Schedule patterns must match four; Notice and RPC have flexible depth. The
broker permits 128 wildcard registrations per domain and session, while exact
registrations do not consume the quota. Lease subscriptions remain exact-only:
`lease://realm/area/resource`.

Notifications expose the exact concrete route. Queue availability events also
include ready, delayed, and inflight message counts. Active registrations are
restored after reconnect and duplicate local registrations share one wire
registration.

Subscriptions use `await client.Notice().SubscribeAsync(pattern)` (and the
equivalent domain method) and return typed handles implementing both
`IAsyncEnumerable<TNotification>` and `IAsyncDisposable`. Each local handle has
a bounded buffer; a slow consumer terminates with
`SubscriptionBackpressureException` without terminating sibling handles.

## Unreleased preview migration

This preview intentionally breaks the earlier callback subscription surface.
Replace callback arguments with `await foreach` over the returned handle. Public
one-shot operations now return `Task`/`Task<T>`; `ValueTask` remains only for
disposal and callback/provider contracts. Schedule listing now returns
`ScheduleListResult` with `Entries` and `TotalCount` properties instead of a
tuple.

## Local Verification

Fast local checks:

```bash
dotnet restore Fitz.sln
dotnet build Fitz.sln -c Release --no-restore
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build --filter "FullyQualifiedName!~Integration"
```

Broker-backed integration and conformance run:

```bash
docker compose up -d
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build
docker compose down --volumes
```

Run a single conformance matrix leg and write the normalized artifact:

```bash
docker compose up -d
CONFORMANCE_TRANSPORT=websocket \
CONFORMANCE_AUTH_MODE=anonymous \
CONFORMANCE_OUTPUT=artifacts/conformance-results.json \
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build --filter FullyQualifiedName~Conformance
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

## Repository Layout

- `src/Core/Core.csproj`: core SDK package and client runtime
- `src/Abstractions/Abstractions.csproj`: shared interfaces and contracts
- `src/DependencyInjection/DependencyInjection.csproj`: DI registration extensions
- `tests/Core/Core.Tests.csproj`: unit, integration, and shared-suite conformance coverage
