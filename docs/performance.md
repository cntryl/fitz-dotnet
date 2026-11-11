# Performance guidance

These rules apply to the client hot paths. Performance numbers are claims only when a
reproducible BenchmarkDotNet result records the runtime, hardware, and job configuration.
The current benchmark suite lives in `bench/Fitz.Benchmarks`; it is not a CI performance gate.

## Async APIs

- Keep public broker operations on `Task`/`Task<T>`: they require network I/O and do not
  complete synchronously.
- Use `ValueTask` for disposal, callback/provider contracts, and internal operations that can
  complete synchronously. Consumers must not need to reason about reusing a `ValueTask`.
- Stream unbounded result sets with `IAsyncEnumerable<T>`; do not collect them in a `List<T>`.
- Use bounded channels and explicit backpressure for concurrent queues. Every queue needs a
  documented bound or an explicit reason it can remain unbounded.
- Use `ConfigureAwait(false)` in library async code.

## Buffers and protocol data

- Parse with `ReadOnlySpan<byte>` and encode with `Span<byte>` where this avoids copies.
- Rent buffers only when pooling reduces measured allocation or pressure. Return each rented
  buffer exactly once, including exceptional and cancellation paths; do not expose a buffer
  after returning it to the pool.
- Validate sizes before allocating or renting, and clear returned buffers when they can hold
  sensitive or consumer-owned data.

## Runtime compatibility

- Keep shipped packages free of runtime reflection. The requirements and whole-program
  Native AOT check are documented in [AOT and reflection](aot-and-reflection.md).
- Prefer measured improvements over assumed runtime or JIT behavior. Avoid hard latency or
  allocation budgets unless a stable benchmark supports them.

Run the benchmarks with:

```bash
dotnet run -c Release --project bench/Fitz.Benchmarks -- --filter '*'
```
