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
        "ws://127.0.0.1:4190/ws",
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

## Local Verification

Fast local checks:

```bash
dotnet restore Fitz.sln
dotnet build Fitz.sln -c Release --no-restore
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build --filter "FullyQualifiedName!~Integration"
```

Broker-backed integration and conformance run:

```bash
docker compose -f compose.yml up -d
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build
docker compose -f compose.yml down --volumes
```

Run a single conformance matrix leg and write the normalized artifact:

```bash
docker compose -f compose.yml up -d
CONFORMANCE_TRANSPORT=websocket \
CONFORMANCE_AUTH_MODE=anonymous \
CONFORMANCE_OUTPUT=artifacts/conformance-results.json \
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build --filter FullyQualifiedName~Conformance
docker compose -f compose.yml down --volumes
```

The conformance artifact uses the shared schema:

- top-level `suite`, `version`, `generated_at`, `client`, `transport`, `auth_mode`, `p0_pass_rate`, `p1_pass_rate`, `overall_status`, and `scenarios`
- 17 scenarios from [conformance/cross-language-conformance-suite.yaml](conformance/cross-language-conformance-suite.yaml)
- one CI artifact per `websocket|tcp` x `anonymous|valid_jwt` leg

## Broker Baseline

[compose.yml](compose.yml) is the repo-owned local broker stack used by CI and local verification.

- `fitz-anon`: `ws://127.0.0.1:4190/ws` and `127.0.0.1:4191`
- `fitz-auth`: `ws://127.0.0.1:4090/ws` and `127.0.0.1:4091`
- default JWT secret: `test-secret-key`
- default JWT audience: `fitz`

## Documentation

- [docs/README.md](docs/README.md)
- [CLIENT_SPEC.md](CLIENT_SPEC.md)
- [CLIENT_ACCEPTANCE_CRITERIA.md](CLIENT_ACCEPTANCE_CRITERIA.md)
- [docs/spec-parity-gap-matrix.md](docs/spec-parity-gap-matrix.md)
- [docs/spec-parity-audit.md](docs/spec-parity-audit.md)

## Repository Layout

- `src/Core/Core.csproj`: core SDK package and client runtime
- `src/Abstractions/Abstractions.csproj`: shared interfaces and contracts
- `src/DependencyInjection/DependencyInjection.csproj`: DI registration extensions
- `tests/Core/Core.Tests.csproj`: unit, integration, and shared-suite conformance coverage
