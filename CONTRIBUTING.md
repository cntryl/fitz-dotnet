# Contributing to fitz-dotnet

## Prerequisites

- The .NET SDK version pinned in [global.json](global.json) (11.0.100; newer feature bands
  are accepted). CI installs exactly what `global.json` selects.
- Docker, for the broker-backed integration and conformance suites.
- Read access to the private `cntryl` GitHub Packages NuGet feed (see below).

## The private NuGet feed

`Cntryl.LexKey` (a runtime dependency of `Cntryl.Fitz.Extensions`) and
`Cntryl.Conventions.Testing.*` (the test-naming analyzers, referenced by every test project)
are only published to `https://nuget.pkg.github.com/cntryl/index.json`, declared in
[NuGet.Config](NuGet.Config). GitHub Packages requires authentication even to read, so
`dotnet restore` fails with `401`/`NU1301` without credentials.

- **cntryl members:** create a personal access token with `read:packages`, then
  `dotnet nuget update source cntryl --username <github-user> --password <token> --store-password-in-clear-text`
  (or set `NuGetPackageSourceCredentials_cntryl="Username=<user>;Password=<token>"` for one shell).
- **GitHub Actions:** the workflows pass `GITHUB_TOKEN` with `packages: read`. A pull request
  from a fork gets a read-only token with no access to the private feed, so **CI cannot
  restore for fork pull requests**; a maintainer has to push the branch to this repository to
  run CI. Fork contributors also cannot build locally until the feed packages are public or
  mirrored; this is a known limitation, not something to work around in the project files.
- **Dependabot** uses the Dependabot secret `DEPENDABOT_PACKAGES_TOKEN` (a `read:packages`
  token); it must exist for NuGet updates to resolve the private packages.

## Before you open a pull request

CI runs all of these. Running them locally first is faster than a round trip.

```bash
dotnet restore Fitz.slnx
dotnet format Fitz.slnx --verify-no-changes --severity warn
dotnet build Fitz.slnx -c Release --no-restore
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build --filter "Category!=Integration"
dotnet test test/Fitz.Analyzers.Tests/Fitz.Analyzers.Tests.csproj -c Release --no-build
dotnet pack Fitz.slnx -c Release --no-build --output artifacts/packages
```

Broker-backed suites (the brokers take a few seconds to listen after `up`; the script waits):

```bash
docker compose up -d
.github/scripts/wait-for-broker.sh
dotnet test test/Fitz.Core.Tests/Fitz.Core.Tests.csproj -c Release --no-build
docker compose down --volumes
```

Tests that need a broker carry `[Trait("Category", "Integration")]`; the cross-language
conformance tests also carry `[Trait("Category", "Conformance")]`. Add the trait to any new
broker-backed test class, or it will run (and fail) in the unit-test job.

## Standards that are enforced, not suggested

**Warnings are errors.** `Directory.Build.props` sets `TreatWarningsAsErrors` with
`AnalysisMode=All` and `EnforceCodeStyleInBuild`. Fix the code. Analyzer suppression of any
kind is not accepted: no `[SuppressMessage]`, no `#pragma warning disable`, no `NoWarn`, and
no `.editorconfig`/globalconfig severity downgrade.

**No runtime reflection.** The shipped packages use none, and
[docs/aot-and-reflection.md](docs/aot-and-reflection.md) is the standing contract: what is
guaranteed, how CI proves it, and the specific patterns to avoid. Read it before touching
enum validation, diagnostics, or dependency injection registration. The whole-program
Native AOT publish will fail the build on a regression.

**Performance patterns.** [docs/performance.md](docs/performance.md) is binding for
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

## Build infrastructure

- **Package versions** live only in [Directory.Packages.props](Directory.Packages.props)
  (central package management); project files reference packages by name. The sole exception
  is `test/Fitz.PackageConsumer`, which opts out because it must consume the freshly packed
  `Cntryl.Fitz.*` at `$(PackageVersion)` from `artifacts/packages` as an external consumer would.
- **Reproducible builds:** `ContinuousIntegrationBuild` is set automatically on GitHub Actions
  (`GITHUB_ACTIONS=true`) and must not be forced locally. SourceLink and `EmbedUntrackedSources`
  are enabled for packable projects.
- **Package validation** (`EnablePackageValidation`) runs on every `dotnet pack`. No
  `PackageValidationBaselineVersion` is set yet, because the next release (4.2.0) deliberately
  breaks the public API. **After 4.2.0 ships, set `PackageValidationBaselineVersion` to
  `4.2.0`** in `Directory.Build.props` so later releases are checked for breaking changes
  against it.
- **`PackageReleaseNotes`** points at `CHANGELOG.md` at the release tag, so it needs no
  per-release edit; the changelog entry and the tag are what matter.
- **Broker image:** `compose.yml` pins the broker by digest and Dependabot proposes bumps.
  The nightly Spec workflow therefore only changes when the pin changes. To test a newer
  broker deliberately, run the Spec workflow manually with its `broker_image` input.

## Releasing

Bump `PackageVersion` in `Directory.Build.props` (the single version source), update
`CHANGELOG.md`, merge to `main`, then run the Publish workflow. It refuses to run if tag
`v<version>` already exists, builds and tests, runs the Native AOT consumer check against the
packed packages, pushes the packages, and only then creates the tag.

## Tests

Behavioral tests use the `ShouldXGivenYWhenZ` naming convention and `// Arrange`,
`// Act`, `// Assert` sections. A fix for a defect needs a test that fails without it.

The cross-language conformance suite in
[conformance/](conformance/cross-language-conformance-suite.yaml) is shared with the other
Fitz clients. Changes there affect every client and need coordination beyond this repo.

## The normative client specification

The Fitz client protocol specification, acceptance criteria, implementation guide, and
connection flow are maintained in the Fitz server repository under `docs/clients`, and they
are normative for this SDK. This repository deliberately keeps no second copy of them: a
fork would drift, and a stale protocol document is worse than no local one. Consult the
server repository directly when you need the wire contract.

The shared suite in [conformance/cross-language-conformance-suite.yaml](conformance/cross-language-conformance-suite.yaml)
and the broker-backed CI runs provide executable compatibility checks.

## Wire compatibility and versioning

`AssemblyVersion` is pinned at `1.0.0.0` permanently so consumer binding never breaks;
releases advance `PackageVersion` only. Do not change `AssemblyVersion`.

The client must not silently emulate a protocol capability the broker does not have. When
a change requires a wire protocol update, coordinate it with the Fitz server contract rather
than working around it client-side.

Record every user-visible change in [CHANGELOG.md](CHANGELOG.md) under `Unreleased`, and
label breaking changes explicitly — including source-breaking ones such as a renamed
parameter.
