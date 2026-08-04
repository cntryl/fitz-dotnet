# Fitz .NET Performance Benchmarks

BenchmarkDotNet-based microbenchmarks for hotpath operations in the Fitz .NET client.

## Running Benchmarks

**Release mode is mandatory for accurate results:**

```bash
cd fitz-dotnet
dotnet run --project tests/Benchmarks/Benchmarks.csproj -c Release
```

**Run specific benchmark:**

```bash
dotnet run -c Release -- --filter Cntryl.Fitz.Benchmarks.FrameCodecBenchmarks.EncodeSmallPayload
```

**Compare against baseline (for regression detection):**

```bash
dotnet run -c Release -- --runtimes net10.0 --baseline
```

## Benchmark Categories

### 1. FrameCodecBenchmarks

Measures TLV frame encoding/decoding latency.

- **EncodeSmallPayload** (64B): Target <100 ns
- **EncodeLargePayload** (1024B): Target <500 ns
- **DecodeFrame**: Target <200 ns (no allocation)

Run all codec benchmarks:
```bash
dotnet run -c Release -- --filter Cntryl.Fitz.Benchmarks.FrameCodecBenchmarks
```

### 1b. FrameParserBenchmarks

Measures parser throughput and allocation behavior for single, batched, and split-frame reads.

- `ParseSingleFrame`
- `ParseTwoFramesBatch`
- `ParseSplitFrameAcrossChunks`

Run parser benchmarks:
```bash
dotnet run -c Release -- --filter Cntryl.Fitz.Benchmarks.FrameParserBenchmarks
```

### 2. MultiplexerBenchmarks

Measures request correlation and dispatch performance under varying concurrency levels.

- **CorrelationLookupUncontended**: Target <200 ns single-threaded
- **CorrelationRegisterUnregister**: Target <1 μs per cycle
- **DispatchResponse**: Target <5 μs with callback overhead

Concurrency levels tested: 10, 100, 1000, 5000

Run all multiplexer benchmarks:
```bash
dotnet run -c Release -- --filter Cntryl.Fitz.Benchmarks.MultiplexerBenchmarks
```

### 2b. MultiplexerHotPathBenchmarks

Measures the real `Multiplexer` hot path from Core instead of synthetic dictionary-only proxies.

- `RequestDispatchRoundTrip`: enqueue request, dispatch frame, complete task
- `CancellationThenNextDispatch`: cancel first inflight request and verify next request still receives response

Run real multiplexer hot-path benchmarks:
```bash
dotnet run -c Release -- --filter Cntryl.Fitz.Benchmarks.MultiplexerHotPathBenchmarks
```

### 3. AllocationBenchmarks

Compares allocation strategies (ArrayPool vs new byte[] vs stackalloc).

- **ArrayPoolAllocation**: Preferred pattern
- **NewByteArrayAllocation**: Eager-allocation baseline
- **StackAllocSpan**: Zero-copy variant

Run all allocation benchmarks:
```bash
dotnet run -c Release -- --filter Cntryl.Fitz.Benchmarks.AllocationBenchmarks
```

## Interpreting Results

### Column Meanings

- **Mean**: Average time per iteration
- **Median**: 50th percentile
- **StdDev**: Standard deviation (lower is better; indicates stability)
- **Allocated**: Heap allocations per operation

### Example Output

```
| Method                    | Mean       | Median     | StdDev     | Allocated |
|---------------------------|------------|------------|------------|-----------|
| EncodeSmallPayload        | 45.23 ns   | 42.15 ns   | 8.67 ns    | 0 B       |
| DecodFrame                | 128.45 ns  | 125.33 ns  | 12.34 ns   | 0 B       |
| CorrelationLookupUncontended | 189.12 ns | 187.45 ns  | 5.67 ns    | 0 B       |
```

### Passing vs Failing Targets

- ✓ **Pass**: Result ≤ target (with +20% tolerance for variance)
- ✗ **Fail**: Result > target (investigate for regression)

## CI Integration

Benchmarks run automatically on:
- Every commit to `main` (release mode)
- Every pull request (comparison mode)

Results are:
1. Compared against baseline (previous main commit)
2. Flagged if regression >10%
3. Stored in `target/bench_summary.md` for trending

## Adding New Benchmarks

1. Create benchmark class inheriting from `[SimpleJob(RuntimeMoniker.Net10)]`
2. Mark methods with `[Benchmark]`
3. Add to appropriate category or create new file
4. Set target latency in docstring (e.g., `/// Target: <100 ns`)
5. Add run instructions to this README

Example:

```csharp
[Benchmark]
public int MyNewBenchmark()
{
    // Implementation
    return result;
}
```

## Performance Targets (Phase 0 Baseline)

| Operation | Target | Status |
|-----------|--------|--------|
| Frame encode (64B) | <100 ns | TBD (after Phase 1) |
| Frame decode | <200 ns | TBD (after Phase 1) |
| Correlation lookup (uncontended) | <200 ns | TBD (after Phase 2) |
| Correlation lookup @ 5K concurrent | <2 μs | TBD (after Phase 2) |
| RPC dispatch per frame | <5 μs | TBD (after Phase 4) |
| Request-response roundtrip | <10 μs | TBD (after Phase 4) |

Status will be updated as implementations complete.

## Troubleshooting

### Benchmark is too slow

1. Ensure **Release** mode (`-c Release`)
2. Check background processes (may interfere with timing)
3. Verify `[MemoryDiagnoser]` isn't degrading results significantly (optional to disable)
4. Run in isolation (single benchmark at a time)

### High StdDev (variance)

- Indicates CPU throttling or system load
- Run again on a quiet machine
- Consider increasing `[SimpleJob]` WarmupCount if <0.5 sec warmup

### Allocated bytes show as 0 but expected allocation

- May indicate GC collection during benchmark
- Rerun to confirm
- Add `// GC: Allocate on LOH` comment for documentation

## References

- [BenchmarkDotNet Documentation](https://benchmarkdotnet.org/)
- [.NET Performance Tips](https://github.com/dotnet/performance/wiki/Benchmarking-workflow-using-BenchmarkDotNet)
- [Fitz Performance Guidelines](../PERF_GUIDELINES.md)

---

**Last Updated:** 2026-03-17  
**Target Framework:** .NET 10.0  
**Expected Baseline:** Establish in Phase 0; improvements tracked in Phase 1–8
