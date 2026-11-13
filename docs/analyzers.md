# Fitz analyzers

`Cntryl.Fitz.Core` ships Roslyn analyzers and code fixes in the same package
(`analyzers/dotnet/cs`), so installing the client turns them on with no second package.
They check the constant values you pass to Fitz operations and fail the build for code that
cannot work at runtime.

| Rule | Category | Severity | What it catches |
|------|----------|----------|-----------------|
| [FITZ001](#fitz001) | Usage | Error | A route literal the client will reject |
| [FITZ002](#fitz002) | Usage | Error | A pattern or selector literal the client will reject |
| [FITZ003](#fitz003) | Reliability | Error | A lease, transaction, subscription, or session that is dropped on the floor |
| [FITZ004](#fitz004) | Usage | Error | A constant limit, batch size, or duration the operation cannot carry |

The analyzers recognise a call to a Fitz client interface (`IKvClient`, `IQueueClient`,
`ILeaseClient`, `INoticeClient`, `IRpcClient`, `IScheduleClient`, `IStreamClient`) however
you reach it: through the interface, through the objects on `FitzClient`, through
`InMemoryKvClient`, or through your own class that implements the interface.

Only constant values are analysed. A route built at runtime is not checked, because the
analyzer cannot know it.

To change a rule's severity in your project, use an `.editorconfig` entry such as
`dotnet_diagnostic.FITZ003.severity = warning`.

## Route grammar

A route is `scheme://segment/segment/...`. Segments are separated by `/`, are never empty,
and may not exceed the 65,535-byte route limit overall. The scheme and segment count depend
on the operation:

| Scheme | Segments in a route | Segments a pattern can match |
|--------|---------------------|------------------------------|
| `kv` | 3 | 3 |
| `queue` | 3 | 3 |
| `lease` | 3 | 3 |
| `notice` | 3 | any |
| `rpc` | any | any |
| `schedule` | 4 | 4 |
| `stream` | 3 | 3 (selectors: see below) |

A concrete route contains no `*`. A pattern may use `*` for exactly one whole segment and
`**` for any number of whole segments; `**` may not be adjacent to another `**`, and a
segment may not mix wildcards with other characters (`bad*value` is invalid). A stream
selector is `stream://**`, `stream://realm/**`, or three segments in which each segment is
either a literal or exactly `*`.

The same source file, `src/Shared/RouteGrammar.cs`, defines these rules for both the
analyzer and the client runtime, so what the analyzer accepts is what the client accepts.

<a id="fitz001"></a>
## FITZ001: Invalid Fitz route

**What it catches.** A constant string passed as the `route` argument of an operation that
needs one concrete address, where the string is not a valid route for that operation.

**Why.** The client validates every route before it touches the network and throws
`ArgumentException` on a bad one. The analyzer reports the same failure at build time. The most common cause is a copied
route with the wrong scheme.

```csharp
await client.Lease.AcquireAsync("queue://realm/area/name", ttl);   // FITZ001: not a lease route
await client.Kv.BeginAsync("kv://realm/area");                      // FITZ001: needs 3 segments
await client.Queue.EnqueueAsync("queue://realm//name", payload);    // FITZ001: empty segment
await client.Kv.BeginAsync("kv://realm/area/*");                    // FITZ001: no wildcards here
```

**How to fix.** Use the operation's scheme, the right number of segments, no empty segments,
and no wildcards. When the only problem is the scheme, the code fix offers to replace it
(`queue://realm/area/name` becomes `lease://realm/area/name`).

<a id="fitz002"></a>
## FITZ002: Invalid Fitz pattern

**What it catches.** A constant string passed as the `pattern` or `selector` argument of a
subscription, listing, or worker registration, where the string is not a valid pattern.

**Why.** Patterns are matched segment by segment on the broker. A wildcard inside a segment,
an empty segment, adjacent `**`, or a pattern that can never match the scheme's segment
count is rejected by the client at runtime.

```csharp
await client.Lease.ListAsync("lease://realm/bad*value/name");       // FITZ002: partial wildcard
await client.Queue.SubscribeAsync("queue://realm/**/**");           // FITZ002: adjacent **
await client.Kv.SubscribeAsync("kv://realm/area");                  // FITZ002: cannot match 3 segments
```

**How to fix.** Use `*` or `**` only as whole segments, never twice in a row, and make sure
the pattern can match a route of the scheme's depth. As with FITZ001, a wrong scheme has a
code fix.

<a id="fitz003"></a>
## FITZ003: Fitz lifecycle handle is discarded

**What it catches.** Awaiting an operation that returns a handle and dropping the result,
either as a bare `await x;` statement or as `_ = await x;`. The handles are leases,
transactions, subscriptions, lease inventory observers, RPC worker registrations, and stream
sessions. The awaited type is matched as a real `Task<T>` or `ValueTask<T>`, not by name.

**Why.** Each handle owns state on the broker (a held lease, an open transaction, a live
subscription) that is released when the handle is disposed. A discarded handle can never be
disposed, so the state is held until the connection closes. For a lease that means the
resource stays locked; for a subscription it means notifications keep arriving with nobody
reading them.

```csharp
await client.Lease.AcquireAsync("lease://realm/area/name", ttl);    // FITZ003
_ = await client.Lease.AcquireAsync("lease://realm/area/name", ttl); // FITZ003
```

**How to fix.** Keep the handle and dispose it:

```csharp
await using var lease = await client.Lease.AcquireAsync("lease://realm/area/name", ttl);
```

The code fix does this. Fix All gives each retained handle a unique local name (`lease`,
`lease2`, ...), and when the discarded call is the unbraced body of an `if` or loop it wraps
the declaration in a block so the code still compiles. Comments and spacing around the
statement are kept. If you really want a handle that lives as long as the connection, hold
it in a field and dispose it with the owner instead of discarding it.

<a id="fitz004"></a>
## FITZ004: Invalid Fitz operation argument

**What it catches.** A constant `limit`, `batchSize`, or duration that the operation cannot
carry:

- `batchSize` must be greater than zero.
- `limit` must not be negative, and `ILeaseClient.ListAsync` requires it to be greater than
  zero.
- `ttl` and `lease` durations must be positive whole seconds, and a `ttl` may not exceed 4,294,967
  seconds (`uint.MaxValue / 1000`).
- `delay` and `wait` durations must not be negative and must be whole seconds.

Durations written as `TimeSpan.Zero` or `TimeSpan.FromDays/FromHours/FromMinutes/FromSeconds/FromMilliseconds`
over a constant are evaluated.

**Why.** The wire carries durations as whole seconds. A sub-second value cannot be
represented and a zero lease would expire immediately, so the client rejects them.

```csharp
await client.Lease.AcquireAsync(route, TimeSpan.FromSeconds(0));          // FITZ004: ttl must be positive
await client.Queue.EnqueueAsync(route, payload, delay: TimeSpan.FromMilliseconds(1)); // FITZ004: whole seconds
await client.Lease.ListAsync(pattern, limit: 0);                          // FITZ004: limit must be positive
```

**How to fix.** Pass a value inside the documented range, in whole seconds.
