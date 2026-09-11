# Contributing to fitz-dotnet

## Prerequisites

- .NET SDK 10.0 or later. `10.0.100` is the minimum supported SDK and CI verifies the
  shipped analyzers load on it.
- Docker, for the broker-backed integration and conformance suites.

## Before you open a pull request

CI runs all of these. Running them locally first is faster than a round trip.

```bash
dotnet restore Fitz.sln
dotnet format Fitz.sln --verify-no-changes --severity warn
dotnet build Fitz.sln -c Release --no-restore
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build --filter "FullyQualifiedName!~Integration"
dotnet test test/Fitz.Analyzers.Tests/Fitz.Analyzers.Tests.csproj -c Release --no-build
```

Broker-backed suites:

```bash
docker compose up -d
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build
docker compose down --volumes
```

## Standards that are enforced, not suggested

**Warnings are errors.** `Directory.Build.props` sets `TreatWarningsAsErrors` with
`AnalysisMode=All` and `EnforceCodeStyleInBuild`. Suppress a diagnostic only with a
justification that says why the rule does not apply — see `GlobalSuppressions.cs` for the
expected form. Do not add a blanket `NoWarn`.

**No runtime reflection.** The shipped packages use none, and
[docs/aot-and-reflection.md](docs/aot-and-reflection.md) is the standing contract: what is
guaranteed, how CI proves it, and the specific patterns to avoid. Read it before touching
enum validation, diagnostics, or dependency injection registration. The whole-program
Native AOT publish will fail the build on a regression.

**Performance patterns.** [PERF_GUIDELINES.md](PERF_GUIDELINES.md) is binding for
hot-path code. Its numeric targets are only as good as the benchmark behind them: do not
quote a performance figure that `bench/Fitz.Benchmarks` does not produce.

**Public API documentation.** Every public type and member carries an XML doc comment.
`GenerateDocumentationFile` is enabled, so a missing one fails the build.

**Cancellation parameters are named `ct`.** This is deliberate and applies to the whole
surface, public and internal, including `<param name="ct">` in doc comments. It is
shorter at the call site than the framework's `cancellationToken` and every Fitz client
uses it. Consistency is the point: do not introduce `cancellationToken` in new code.

The sole exception is a member implementing a BCL interface that fixes the name, such as
`IAsyncEnumerable<T>.GetAsyncEnumerator`. CA1725 enforces the match and the build fails
otherwise; the one such member carries a comment saying so.

## Tests

Behavioral tests use the `ShouldXGivenYWhenZ` naming convention and `// Arrange`,
`// Act`, `// Assert` sections. A fix for a defect needs a test that fails without it.

The cross-language conformance suite in
[conformance/](conformance/cross-language-conformance-suite.yaml) is shared with the other
Fitz clients. Changes there affect every client and need coordination beyond this repo.

## Wire compatibility and versioning

`AssemblyVersion` is pinned at `1.0.0.0` permanently so consumer binding never breaks;
releases advance `PackageVersion` only. Do not change `AssemblyVersion`.

The client must not silently emulate a protocol capability the broker does not have. When
a change requires the wire protocol to move, record it as `protocol-deferred` in
[docs/sharp-edges-evidence-ledger.md](docs/sharp-edges-evidence-ledger.md) rather than
working around it client-side.

Record every user-visible change in [CHANGELOG.md](CHANGELOG.md) under `Unreleased`, and
label breaking changes explicitly — including source-breaking ones such as a renamed
parameter.
