# Using the Fitz .NET client

A task-oriented guide for applications consuming `Cntryl.Fitz.Core`. For the wire protocol itself,
see the Fitz server repository under `docs/clients`; this document covers only what the .NET
client asks of you.

## Contents

- [Install and connect](#install-and-connect)
- [Routes](#routes)
- [Cancellation and disposal](#cancellation-and-disposal)
- [Durations](#durations)
- [Errors and retry](#errors-and-retry)
- [Subscriptions](#subscriptions)
- [Reconnect](#reconnect)
- [The seven domains](#the-seven-domains)
- [Dependency injection and hosting](#dependency-injection-and-hosting)
- [Observability](#observability)
- [Trimming and Native AOT](#trimming-and-native-aot)

## Install and connect

```bash
dotnet add package Cntryl.Fitz.Core
dotnet add package Cntryl.Fitz.Abstractions
dotnet add package Cntryl.Fitz.DependencyInjection   # only if you use IServiceCollection
```

`Cntryl.Fitz.Core` carries the Roslyn analyzers and their code fixes, so the route and
handle-lifetime diagnostics are on as soon as it is installed.

The entire public surface is in one namespace, so one using directive covers everything except
the DI helpers:

```csharp
using Cntryl.Fitz;

await using var client = new Client(
    new ClientConfig(
        new Uri("ws://127.0.0.1:4190/ws"),
        TokenProvider: _ => ValueTask.FromResult("your-jwt-token")));

await client.ConnectAsync(ct);
```

`ConnectAsync` makes one attempt and throws on failure. At process startup, where the broker
may not be up yet, use `ConnectWhenReadyAsync`, which retries transient failures with bounded
backoff and leaves authentication rejections terminal:

```csharp
await client.ConnectWhenReadyAsync(
    new ConnectWhenReadyOptions(Timeout: TimeSpan.FromSeconds(30)), ct);
```

If it gives up, it throws `TimeoutException` with the last underlying failure as the inner
exception — check `InnerException` before assuming the broker was merely slow.

`ClientConfig` is validated eagerly, so a bad configuration throws at construction rather than
on first use. `ClientTransport.Auto` (the default) picks WebSocket or TCP from the URL scheme.

## Routes

Every domain addresses resources by route, a URI of the form `scheme://realm/area/name`:

```
kv://prod/billing/invoices
queue://prod/billing/outbox
lease://prod/billing/leader
```

Subscriptions take patterns instead, where `*` matches one segment and `**` matches the rest:

```
kv://prod/billing/*
stream://**
```

Routes are validated client-side before anything reaches the broker. The shipped Roslyn
analyzer (`Cntryl.Fitz.Analyzers`, referenced automatically by the `Cntryl.Fitz` package)
checks literal routes at compile time, so a malformed route or a pattern passed where a
concrete route is required is a build error rather than a runtime one. It is build-time only
and never ships into your application.

## Cancellation and disposal

Every asynchronous operation takes a `CancellationToken` named `ct`, always last. The
exception is `DisposeAsync`, which takes none because `IAsyncDisposable` defines it that way.
Caller cancellation surfaces as `OperationCanceledException`, never as a Fitz exception.

Handles are `IAsyncDisposable` and must be awaited with `await using`, not `using`:

```csharp
await using var tx = await client.Kv.BeginAsync(route, KvDurability.Sync, ct: ct);
```

`Client.CloseAsync` is the explicit, idempotent shutdown. `DisposeAsync` delegates to it, so
`await using var client = ...` is sufficient.

## Durations

Every duration in the API is a `TimeSpan` — there are no `*Secs` or `*Ms` parameters. The Fitz
wire carries whole seconds for lease TTLs, reservation leases, and waits, so a duration with
finer precision than the wire can represent is **rejected rather than rounded**:

```csharp
await lease.ExtendAsync(TimeSpan.FromSeconds(30));      // fine
await lease.ExtendAsync(TimeSpan.FromMilliseconds(1500)); // ArgumentOutOfRangeException
```

Rounding 1500 ms down to one second would hand back a shorter hold than you asked for, and
that difference only surfaces under contention. The analyzer evaluates `TimeSpan.FromSeconds`
and friends at compile time, so a bad literal is a build error rather than a runtime one.

## Errors and retry

Most client exceptions derive from `FitzException`. The per-domain types — `KvException`,
`QueueException`, `RpcException`, `LeaseException`, `StreamException`, `NoticeException`,
`ScheduleException` — carry the broker's structured error code, so classify on the code and
never on message text:

```csharp
catch (RpcException ex) when (ex.DomainCode == FitzErrorCodes.RpcTimeout)
{
    // the worker did not answer in time
}
```

`FitzErrorCodes` lists every code the broker defines. Transport and lifecycle problems surface
as `ConnectionException`, `AuthenticationException`, `RequestTimeoutException`,
`RequestQueueFullException`, or `ProtocolException`.

Two exceptions sit outside that hierarchy and derive from `Exception` directly:
`SubscriptionBackpressureException` and `AsyncHandlerOverflowException`. They are declared in
`Cntryl.Fitz.Abstractions`, which does not reference the assembly `FitzException` lives in. A
`catch (FitzException)` will not catch either, so catch them explicitly where you consume a
subscription.

The client retries internally, and only where retrying is safe: replayable reads are retried
with jittered exponential backoff under a single deadline shared by all attempts, while
session-bound mutations and authentication stay terminal. Configure it with `RetryOptions`, or
disable it and decide yourself:

```csharp
if (Retryability.IsRetryable(ex)) { /* your own policy */ }
```

`RequestQueueFullException` means the client, not the broker, shed the request:
`MaxInFlightRequests` and `MaxRequestQueueSize` bound how much work can be outstanding. Treat
it as backpressure and slow down.

## Subscriptions

Every `SubscribeAsync` returns a handle that is both an async sequence and a lifetime:

```csharp
await using var subscription = await client.Notice.SubscribeAsync("notice://prod/app/*", ct);

await foreach (var message in subscription.WithCancellation(ct))
{
    Handle(message);
}
```

Delivery is bounded. Callbacks run on a dedicated pump rather than on the transport receive
loop, so a slow consumer cannot stall the connection — but it can overflow its own buffer,
which surfaces as `SubscriptionBackpressureException` or `AsyncHandlerOverflowException` and
terminates that subscription rather than silently dropping records. Size the buffer and
concurrency with `AsyncHandlerOptions`.

`subscription.Completion` completes when the subscription ends, and faults with the reason if
it ended abnormally. Await it if you need to observe that.

## Reconnect

Reconnection is automatic and configured with `ReconnectOptions`. Active subscriptions and RPC
worker registrations are restored afterward; if restoration fails, the session is failed and
retried rather than reported healthy.

Handles obtained before a disconnect do not survive it. A `IKvTransaction`, `ILease`, or
`IStreamSession` is session-bound: after a reconnect it is invalid, and using it throws. Acquire
a new one. To react directly:

```csharp
client.State        // Disconnected, Connecting, Connected, Authenticating, Authenticated, Reconnecting, Closed
client.IsConnected  // true only in Authenticated
```

Fitz has no application-level heartbeat frame. Silent transport loss is detected by TCP
keepalive and WebSocket PING/PONG, configured through `HeartbeatOptions`.

## The seven domains

### KV — transactional key/value

```csharp
await using var tx = await client.Kv.BeginAsync(
    "kv://prod/billing/invoices", KvDurability.Sync, KvMode.ReadWrite, ct);

await tx.PutAsync(key, value, ct);
var result = await tx.GetAsync(key, ct);
if (result.Found) { Use(result.Value); }

await tx.CommitAsync(ct);
```

Durability is explicit and has no default: `Sync` waits for the write to be durable, `Async`
does not. Without a `CommitAsync` the transaction rolls back on disposal. `ScanAsync` returns
a `KvScanResult` whose `HasMore` tells you to continue from the last key.

### Queue — work distribution

```csharp
await client.Queue.EnqueueAsync("queue://prod/billing/outbox", body, ct: ct);

var items = await client.Queue.ReserveAsync(
    "queue://prod/billing/outbox", TimeSpan.FromSeconds(30), batchSize: 10, ct: ct);

foreach (var item in items)
{
    await Process(item.Body);
    await item.CompleteAsync(ct);
}
```

A reserved item not completed within its lease returns to the queue. Call `ExtendAsync` if
processing legitimately runs long.

`item.Attempt` exists but is not usable today: the current queue wire carries no attempt
count, so it is always `QueueItem.AttemptUnavailable` (`0`). Do not build redelivery logic on
it.

### Notice — fire-and-forget pub/sub

```csharp
await client.Notice.PublishAsync("notice://prod/app/deployed", body, ct);
```

No delivery guarantee and no persistence: subscribers connected at publish time receive it.

### RPC — request/response with streaming

```csharp
await foreach (var frame in client.Rpc.CallAsync("rpc://prod/app/resize", body, ct))
{
    Accumulate(frame.Body);
}
```

Serving:

```csharp
await using var worker = await client.Rpc.RegisterWorkerAsync(
    "rpc://prod/app/*",
    async (request, response, ct) =>
    {
        await response.SendAsync(Handle(request.Body), isEnd: true, ct);
    },
    ct: ct);
```

A response is a stream: send as many frames as you like, and mark the last with `isEnd: true`.
Worker registrations are restored across reconnects.

### Lease — distributed mutual exclusion

Prefer the managed form, which owns acquisition, renewal, cancellation, and release:

```csharp
await client.Lease.WithLeaseAsync(
    "lease://prod/billing/leader", TimeSpan.FromSeconds(30),
    async (authority, leaseCt) =>
    {
        while (!leaseCt.IsCancellationRequested)
        {
            await DoLeaderWork(authority.FencingToken, leaseCt);
        }
    },
    ct: ct);
```

The callback token is cancelled the moment the client observes a disconnect or any other loss
of lease ownership — honour it promptly. `authority.FencingToken` is the monotonic token from
the successful acquire; pass it to any downstream system that needs to reject a stale holder.

Implementing `ILeaseClient` yourself (a test double, say) means writing the two
authority-aware `WithLeaseAsync` overloads; the cancellation-only pair is supplied by the
interface and discards the fence. No member throws `NotSupportedException`.

`AcquireAsync` returns a low-level `ILease` if you need to manage the lifetime yourself. To wait
out contention rather than failing immediately, pass `LeaseExecutionOptions`. Note that it is a
class with init-only properties, not a positional record like the other options types, so it
takes an object initializer:

```csharp
var options = new LeaseExecutionOptions { WaitForAvailability = true, Wait = TimeSpan.FromSeconds(30) };
```

### Stream — ordered, offset-addressed records

```csharp
await foreach (var record in client.Stream.ReadAsync(route, startOffset: 0, ct: ct))
{
    Handle(record.Body.Span, record.Offset);
}
```

Appending is transactional through a session:

```csharp
await using var session = await client.Stream.BeginAsync(route, ct: ct);
await session.AppendAsync(expectedOffset, body, ct: ct);
await session.CommitAsync(ct);
```

`AppendAsync` takes the offset you expect to write at; a mismatch is an optimistic-concurrency
failure, reported as `StreamException` with `DomainCode` 2001. Classify on that code, never on
the message.

Use `ReadPageAsync` when you need to see what the broker withheld: a `StreamReadPage` contains
`StreamReadItem`s whose `Kind` distinguishes delivered records from filtered ones, so gaps in
offsets are explicit rather than something you infer. A filtered item's `Reason` is
`StreamFilteredReason.None` when the broker withheld it without saying why; a null `Reason`
means the item is not a filtered one at all.

### Schedule — cron-driven delivery

```csharp
var id = await client.Schedule.CreateAsync(
    "schedule://prod/billing/nightly", "0 2 * * *", ScheduleDeliveryMode.Once, payload, ct);
```

`Broadcast` delivers each firing to every current subscriber; `Once` delivers it to exactly
one. `ListAsync(offset, limit)` pages through schedules and returns `TotalCount` alongside the
page.

## Dependency injection and hosting

```csharp
services.AddFitzClient(new ClientConfig(new Uri("ws://127.0.0.1:4190/ws")));
```

This registers `ClientConfig`, `Client`, and an `IHostedService` that connects on start and
closes on stop, all through explicit factories rather than container type activation, so
nothing reflects over your types at startup. Inject `Client` (or `IClient`) wherever you need
it; it is a singleton and safe to use concurrently.

## Observability

The core takes no logging dependency. Supply your own through `FitzObservabilityOptions`:

```csharp
new ClientConfig(
    url,
    Observability: new FitzObservabilityOptions(
        Logger: myLogger,      // IFitzLogger
        Tracer: myTracer,      // IFitzTracer
        Meter: myMeter,        // IFitzMeter
        OnLifecycleEvent: e => ...));
```

Lifecycle events cover connect, authentication rejection, connection loss, reconnect, retry,
timeout, and queue saturation. Instrumentation is guarded, so a throwing logger cannot corrupt
connection state. The client also emits standard `System.Diagnostics` activities.

If you supply a custom `ITransport` via `ClientConfig.TransportFactory`, override
`ITransport.TransportName` with a constant so it is labelled in telemetry; it defaults to
`"custom"` rather than inspecting the runtime type.

## Trimming and Native AOT

All three runtime packages are trim-safe and Native-AOT-safe with no configuration on your
side. They use no reflection on any code path and declare no dynamic-access contracts. See
[aot-and-reflection.md](aot-and-reflection.md) for how that is enforced and what to avoid if
you contribute.
