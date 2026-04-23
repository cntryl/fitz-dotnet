# fitz-dotnet

`fitz-dotnet` is the .NET SDK for Fitz.

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
        "ws://localhost:4190/ws",
        TokenProvider: _ => ValueTask.FromResult("your-jwt-token")
    )
);

await client.ConnectAsync();

var tx = await client.Kv().BeginAsync("kv://realm/app/users");
await tx.PutAsync("user-1"u8.ToArray(), """{"name":"Alice"}"""u8.ToArray());
await tx.CommitAsync();

await client.DisposeAsync();
```

## Verification

Fast local checks:

```bash
dotnet restore Fitz.sln
dotnet build Fitz.sln -c Release --no-restore
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build
```

Full broker-backed test run:

```bash
docker compose -f ../fitz-go/compose.yml up -d
dotnet test tests/Core/Core.Tests.csproj -c Release --no-build
docker compose -f ../fitz-go/compose.yml down --volumes
```

`dotnet test` now includes the broker-backed integration and conformance tests by default. The conformance suite writes JSON results to `artifacts/conformance-results.json` by default.

## Documentation

- [docs/README.md](docs/README.md)
- [CLIENT_SPEC.md](CLIENT_SPEC.md)
- [CLIENT_ACCEPTANCE_CRITERIA.md](CLIENT_ACCEPTANCE_CRITERIA.md)

## Repository Layout

- `src/Core/Core.csproj`: core SDK package and client implementation
- `src/Abstractions/Abstractions.csproj`: shared interfaces and contracts
- `src/DependencyInjection/DependencyInjection.csproj`: DI registration extensions
- `tests/Core/Core.Tests.csproj`: unit, integration, and broker-backed conformance coverage
