# Fitz .NET

Fitz .NET is the .NET 11 client for the [Fitz broker](https://github.com/cntryl/fitz).
Use it for transactional key/value storage, queues, streaming RPC, leases,
notices, event streams, and schedules over Wire 3. The client supports TCP and
WebSocket. See the [Wire 3 migration guide](docs/wire3-migration.md) for broker
compatibility and upgrade details.

## Install

Packages are available from the Cntryl GitHub Packages feed. See
[feed and credential setup](CONTRIBUTING.md#the-private-nuget-feed).

```sh
dotnet add package Cntryl.Fitz.Core
```

| Package | Use |
| --- | --- |
| `Cntryl.Fitz.Core` | Client runtime with bundled route and lifetime analyzers and code fixes. |
| `Cntryl.Fitz.Abstractions` | Client interfaces and contracts for libraries that only need the API surface. |
| `Cntryl.Fitz.DependencyInjection` | Register a client and manage its lifecycle with the .NET Generic Host. |
| `Cntryl.Fitz.Extensions` | Indexed KV directories with bounded queries and keyset pagination. |
| `Cntryl.Fitz.Testing` | In-memory KV implementation for deterministic consumer tests. |

## Quick start

From a checkout, start the local broker and wait until it accepts connections.

```sh
docker compose up -d
.github/scripts/wait-for-broker.sh
```

The local anonymous WebSocket broker listens on port 4190. For a broker that
requires authentication, configure `ClientConfig.TokenProvider`.

```csharp
using Cntryl.Fitz;

await using var client = new FitzClient(
    new ClientConfig(new Uri("ws://127.0.0.1:4190/ws")));

await client.ConnectWhenReadyAsync();

await using var transaction = await client.Kv.BeginAsync("kv://local/app/users");
await transaction.PutAsync("user-1"u8.ToArray(), """{"name":"Alice"}"""u8.ToArray());
await transaction.CommitAsync(KvDurability.Sync);
```

## Guides

| Task | Guide |
| --- | --- |
| Connect, configure retries, or use a client domain | [Client guide](docs/guide.md) |
| Connect with AWS, Entra or Kubernetes workload identity | [Workload identity example](examples/Fitz.WorkloadIdentity/README.md) |
| Build or test a KV directory | [Indexed directories](docs/guide.md#indexed-kv-directories) · [Testing KV consumers](docs/guide.md#testing-kv-consumers) |
| Control subscription delivery or handle reconnects | [Subscriptions](docs/guide.md#subscriptions) · [Reconnect](docs/guide.md#reconnect) |
| Configure hosting and telemetry | [Dependency injection](docs/guide.md#dependency-injection-and-hosting) · [Observability](docs/guide.md#observability) |
| Verify a trimmed or AOT deployment | [Trimming and Native AOT](docs/aot-and-reflection.md) |
| Check wire behavior | [Protocol specification](https://github.com/cntryl/fitz/tree/main/docs/clients/spec) · [Wire 3 migration](docs/wire3-migration.md) |

## Development

Run tests without starting a broker.

```sh
dotnet test Fitz.slnx -c Release --filter 'Category!=Integration'
```

See [Contributing](CONTRIBUTING.md) for broker-backed tests and
[conformance](CONTRIBUTING.md#run-one-conformance-leg). The
[documentation index](docs/README.md) lists the engineering guides. Release history
is in the [changelog](CHANGELOG.md).

Fitz .NET is licensed under [Apache 2.0](LICENSE).
