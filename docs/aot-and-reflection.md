# Trimming, Native AOT, and zero runtime reflection

This document is the standing contract for `Cntryl.Fitz`, `Cntryl.Fitz.Abstractions`,
and `Cntryl.Fitz.DependencyInjection`. It states what the packages guarantee, how the
guarantee is enforced mechanically, and what a contributor must not reintroduce.

`Cntryl.Fitz.Analyzers` and `Cntryl.Fitz.CodeFixes` are build-time Roslyn components.
They never ship into a consumer's application and are outside this contract.

## The guarantee

1. **No runtime reflection on any code path.** Not on startup, not on the hot path, not
   on error paths. The shipped assemblies contain no `System.Reflection` use, no
   `Activator`, no `Type.Get*`, no expression compilation, no dynamic code generation,
   no serializers, and no P/Invoke.
2. **No reflection contracts either.** The assemblies declare no
   `DynamicallyAccessedMembers`, `RequiresUnreferencedCode`, `RequiresDynamicCode`, or
   `UnconditionalSuppressMessage` annotations, and suppress no trim or AOT warning.
   Nothing in the libraries asks the trimmer to preserve members it would otherwise
   remove.
3. **Zero `IL2xxx`/`IL3xxx` diagnostics under whole-program analysis**, with every
   method of all three assemblies rooted — not only the subset a sample application
   happens to call.

There are no documented exceptions. Telemetry naming was the last one: transports now
report themselves through `ITransport.TransportName`, whose interface default is the
constant `"custom"`. The built-in transports override it with `nameof`, and an
application supplying its own transport through `ClientConfig.TransportFactory` can do
the same. Nothing in the client calls `GetType()`.

Adding `TransportName` as a default interface member keeps every existing `ITransport`
implementation compiling and binary-compatible, consistent with `PKG-8` in the
[sharp-edges evidence ledger](sharp-edges-evidence-ledger.md).

## How it is enforced

### Build time

All three packages set `IsAotCompatible=true`, which turns on the trim, AOT, and
single-file Roslyn analyzers. `Directory.Build.props` sets `TreatWarningsAsErrors=true`,
so any analyzer-visible violation fails the build.

### Publish time

Roslyn analyzers reason one method at a time and **do not catch everything** — see the
worked example below. The authoritative check is the whole-program ILC analysis run by
the `package` CI job, which publishes `test/Fitz.PackageConsumer` as a Native AOT
executable against the freshly packed NuGet artifacts.

That project configures the proof:

```xml
<TrimmerSingleWarn>false</TrimmerSingleWarn>
<ILLinkTreatWarningsAsErrors>true</ILLinkTreatWarningsAsErrors>

<ItemGroup Condition="'$(PublishAot)' == 'true'">
  <TrimmerRootAssembly Include="Cntryl.Fitz.Core" />
  <TrimmerRootAssembly Include="Cntryl.Fitz.Abstractions" />
  <TrimmerRootAssembly Include="Cntryl.Fitz.DependencyInjection" />
</ItemGroup>
```

Each line matters:

- **`TrimmerRootAssembly`** forces ILC to analyze every method in the shipped
  assemblies. Without it the analysis covers only what the sample's `Main` reaches, and
  a reflection dependency in an unexercised code path publishes clean.
- **`TrimmerSingleWarn=false`** reports each finding with its originating method instead
  of collapsing an assembly into one summary warning.
- **`ILLinkTreatWarningsAsErrors=true`** makes a regression fail CI rather than scroll
  past in a green log.

### Reproducing locally

Four steps, matching what the `package` CI job runs. Substitute your own runtime
identifier for `linux-x64`:

```bash
dotnet pack Fitz.sln -c Release --output artifacts/packages
rm -rf artifacts/consumer-package-cache
dotnet publish test/Fitz.PackageConsumer/Fitz.PackageConsumer.csproj \
  -c Release -r linux-x64 -p:PublishAot=true --output artifacts/native-aot
artifacts/native-aot/Fitz.PackageConsumer
```

A clean run prints `Generating native code` and emits no `IL` diagnostics.

**Do not skip the second step.** `test/Fitz.PackageConsumer/NuGet.Config` maps
`Cntryl.Fitz*` to `artifacts/packages` so the publish consumes the packages just built —
but `PackageVersion` is intentionally fixed across rebuilds, and NuGet keys its extracted
cache on package id and version alone. A copy of that version left from an earlier pack
therefore shadows the one just produced, and the verification passes against stale bits
with no indication anything is wrong. This is not hypothetical: it happened during this
audit.

The consumer sets `RestorePackagesPath` to `artifacts/consumer-package-cache` so it never
reads the shared global package cache. That is what makes the `rm -rf` both safe — it
cannot touch packages other projects depend on — and cheap.

## Why whole-assembly rooting is the requirement

The audit that produced this document found `IL2091` in `Client.GetDomain<T>(Lazy<T>)`:

```csharp
T GetDomain<T>(Lazy<T> domain)   // removed
{
    ThrowIfDisposed();
    return domain.Value;
}
```

`Lazy<T>` annotates its type parameter with
`DynamicallyAccessedMemberTypes.PublicParameterlessConstructor`, because its
parameterless overload constructs `T` through `Activator.CreateInstance<T>()`. Passing
an unannotated `T` into that position propagates a reflection contract. No call site
used the reflecting overload — every domain client is created by an explicit value
factory — but the trimmer must assume the annotation is meaningful.

This shipped in `0.1.3`. It was invisible to the in-build analyzers and invisible to the
CI AOT publish, because the sample never read a domain property. Rooting every method
surfaced it immediately.

The fix removed the generic hop rather than annotating it. Adding
`[DynamicallyAccessedMembers]` would have silenced the warning while making the
reflection contract permanent and forcing the trimmer to preserve constructors nothing
calls. Each domain property now guards and dereferences its own `Lazy<T>` field, whose
type argument is concrete.

**Prefer deleting a reflection contract over annotating it.** Reach for an annotation
only when the reflection is real and necessary; in this codebase, it never has been.

## Rules for contributors

**Enum validation.** Do not use `Enum.IsDefined` or `Enum.GetValues`. They resolve
through cached runtime enum metadata. Use an explicit membership test:

```csharp
// No
if (!Enum.IsDefined(mode)) { throw ... }

// Yes
if (mode is not KvMode.ReadOnly and not KvMode.ReadWrite) { throw ... }
```

**Enum names in messages.** Do not call `ToString()` on an enum or interpolate one into
a string — both go through runtime metadata and keep the enum name tables alive. Map to
`nameof` explicitly, with a numeric fallback for undefined values:

```csharp
static string Describe(ClientTransport transport) => transport switch
{
    ClientTransport.Auto => nameof(ClientTransport.Auto),
    ClientTransport.WebSocket => nameof(ClientTransport.WebSocket),
    ClientTransport.Tcp => nameof(ClientTransport.Tcp),
    _ => ((int)transport).ToString(CultureInfo.InvariantCulture),
};
```

A `switch` over every member also makes a newly added enum member a visible edit rather
than a silently degraded message.

**Type names.** Do not call `GetType()` for diagnostics. When a polymorphic value needs
a label, put the label on the abstraction as a constant-returning member, as
`ITransport.TransportName` does, and give it a default so implementers outside this repo
keep compiling.

**Dependency injection.** Register with explicit factories, never the type-based
overloads:

```csharp
// No — the container selects and invokes the constructor reflectively.
services.AddSingleton<Client>();

// Yes
services.AddSingleton(static sp => new Client(sp.GetRequiredService<ClientConfig>()));
```

The type-based overloads are trim-*compatible* — `Microsoft.Extensions.DependencyInjection.Abstractions`
annotates them — but being compatible means the constructors are preserved and invoked
reflectively at runtime. An explicit factory removes the reflection instead of
annotating it.

**Generic type parameters.** Before flowing a generic `T` into a BCL generic, check
whether the target annotates its type parameter. `Lazy<T>` and `ActivatorUtilities` are
the ones this codebase has hit. If the local analyzers stay quiet, the whole-program
publish is the check that counts.

**Serialization.** None of the shipped types implement `ISerializable` or carry
serialization constructors, and the wire protocol is hand-written binary
(`BinaryBufferReader`/`BinaryBufferWriter`). Do not introduce a reflection-based
serializer. If JSON is ever required, use a source-generated
`JsonSerializerContext` — never the reflection-based `JsonSerializer` overloads.

## Audit result

Whole-program ILC analysis with all three assemblies rooted reports zero trim and zero
AOT diagnostics, and the published Native AOT executable runs correctly. The
remediations that produced that result:

| Area | Change |
|---|---|
| `Client` | Removed `GetDomain<T>(Lazy<T>)`; each domain property guards and reads its own field. Clears `IL2091`. |
| `ClientConfig`, `KvClient`, `StreamWireHelpers` | Replaced `Enum.IsDefined` with explicit membership tests. |
| `TransportResolver`, `FitzConnection` | Replaced enum interpolation with `nameof` maps. |
| `ITransport`, `FitzConnection` | Transports self-report a constant `TransportName`; the last `GetType()` call is gone. |
| `ServiceCollectionExtensions` | Replaced container type activation with explicit factories. |
| `Fitz.PackageConsumer` | Roots all shipped assemblies, expands per-finding warnings, promotes trim/AOT warnings to errors, and restores into a dedicated package cache. |
| CI | Clears the consumer package cache before the AOT publish, which would otherwise verify stale bits. |
