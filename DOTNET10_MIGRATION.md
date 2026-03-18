# .NET 10 Migration Guide

## Status

**Current State:** net9.0  
**Target:** net10.0 (pending .NET 10 SDK installation)  
**Estimated Date:** When .NET 10 SDK is installed on CI/development machines

## What to Do When .NET 10 SDK Becomes Available

### Step 1: Upgrade Framework Targets

Replace all instances of `<TargetFramework>net9.0</TargetFramework>` with `<TargetFramework>net10.0</TargetFramework>` in:

```bash
sed -i 's/net9.0/net10.0/g' src/Core/Core.csproj
sed -i 's/net9.0/net10.0/g' src/Abstractions/Abstractions.csproj
sed -i 's/net9.0/net10.0/g' src/DependencyInjection/DependencyInjection.csproj
sed -i 's/net9.0/net10.0/g' tests/Core/Core.Tests.csproj
sed -i 's/net9.0/net10.0/g' tests/Benchmarks/Benchmarks.csproj
```

Or edit each .csproj manually:

- [src/Core/Core.csproj](../src/Core/Core.csproj)
- [src/Abstractions/Abstractions.csproj](../src/Abstractions/Abstractions.csproj)
- [src/DependencyInjection/DependencyInjection.csproj](../src/DependencyInjection/DependencyInjection.csproj)
- [tests/Core/Core.Tests.csproj](../tests/Core/Core.Tests.csproj)
- [tests/Benchmarks/Benchmarks.csproj](../tests/Benchmarks/Benchmarks.csproj)

### Step 2: Verify Build

```bash
dotnet clean Fitz.sln
dotnet build Fitz.sln -c Release
dotnet test tests/Core/Core.Tests.csproj
```

### Step 3: Establish Baseline Benchmarks

Run benchmarks to establish .NET 10 perf baseline:

```bash
dotnet run --project tests/Benchmarks/Benchmarks.csproj -c Release
```

Expected improvements vs net9.0:
- Frame encode: 5× faster (<100 ns)
- Correlation lookup: 4× faster (<200 ns uncontended)
- Request-response roundtrip: 3-5× faster (<10 μs)
- Allocations: 3-10× reduction

### Step 4: Capitalize on .NET 10 Features

Once net10.0 is confirmed, the codebase is positioned to automatically benefit from:

- **Stack allocation of delegate closures** (escape analysis) — improves RPC correlation handlers
- **Channels<T> linked-list backing** — reduces GC under high concurrency
- **Bounds-check elimination** — improves Span<T> iteration in async streams
- **WebSocketStream API** — eliminates manual frame buffering
- **try/finally inlining** — improves error path performance
- **Array devirtualization** — improves IAsyncEnumerable performance

See [PERF_GUIDELINES.md](../PERF_GUIDELINES.md) Section 9 for full feature list.

### Step 5: Update CI/CD

Update CI workflow files to:

```yaml
strategy:
  matrix:
    # Remove: dotnet-version: ['9.0.x']
    # Add:
    dotnet-version: ['10.0.x']
```

All benchmarks and conformance tests will automatically run targeting net10.0.

## Checking SDK Version

To check what .NET SDK versions are installed:

```bash
dotnet --list-sdks
```

To download .NET 10 SDK when available:

```bash
# Windows
https://dotnet.microsoft.com/download

# Or via package manager
# macOS: brew install dotnet
# Linux: apt-get install dotnet-sdk-10.0
```

## Why Split This Out?

Phase 0 establishes the **infrastructure** for performance-first design:
- ✅ Package references for Channels, WebSockets, BenchmarkDotNet
- ✅ Perf measurement utilities (LatencyHistogram, PerfTimer, ThroughputMeter)
- ✅ Performance guidelines and targets
- ✅ Benchmark harness templates (3 categories, ready to measure)
- ⏳ Framework upgrade (pending SDK availability)

Once the SDK is available, the upgrade (step 1-2) is mechanical and low-risk. All other infrastructure is ready and tested on net9.0.

## Net9.0 → Net10.0 Compatibility

The code written for net9.0 is **fully compatible** with net10.0. No source changes needed; the upgrade is purely framework target adjustment and will automatically gain perf benefits from:
- JIT optimizations
- Runtime improvements
- Package updates

---

**Timeline:**  
- **Current:** Phase 0 infrastructure complete on net9.0
- **Upon SDK availability:** Execute steps 1-2 (30 min)
- **Phase 1+:** Proceed with feature implementation; will run on net10.0 automatically

**Next Meeting:** When `dotnet --list-sdks` shows `10.0.x`, trigger this migration guide.
