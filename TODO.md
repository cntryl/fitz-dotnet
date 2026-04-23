# Fitz Dotnet World-Class TODO

You are working in fitz-dotnet. Your job is to close the remaining Fitz parity gaps and turn this SDK into a world-class client without redesigning the public surface unless the canonical Fitz contract requires it.

## Canonical Sources

- [../fitz/docs/clients/client-spec.md](../fitz/docs/clients/client-spec.md)
- [../fitz/docs/clients/client-acceptance-criteria.md](../fitz/docs/clients/client-acceptance-criteria.md)
- [../fitz/docs/clients/client-implementation-guide.md](../fitz/docs/clients/client-implementation-guide.md)
- [../fitz/docs/clients/connection-flow.md](../fitz/docs/clients/connection-flow.md)
- [docs/spec-parity-audit.md](docs/spec-parity-audit.md)
- [docs/spec-parity-gap-matrix.md](docs/spec-parity-gap-matrix.md)
- [README.md](README.md)

## What Is Still Missing

- The repo still leans on a smoke-style conformance shape instead of a contract-compliant shared runner for `CS-001` through `CS-015`.
- Auth and connection lifecycle need to be proven against broker reality, not just local settle-delay behavior.
- The public abstractions still do not fully represent the Fitz contract for RPC, Queue, Notice, Schedule, Stream, and the remaining KV behavior gaps.
- TCP and WebSocket must both be exercised in the shared conformance matrix, not only implemented as transport options.
- Reconnect, shutdown, timeout, cancellation, and same-type concurrency need stronger end-to-end proof.
- The conformance output and repo documentation still need to match the shared runner contract exactly.

## Work In Order

1. Replace the smoke conformance flow with a dedicated shared-suite runner.
   - Execute every scenario from `CS-001` to `CS-015`.
   - Accept transport and auth inputs from the environment.
   - Emit normalized JSON matching the shared runner contract.
   - Gate CI on the correct P0/P1 behavior and retain artifacts for all supported transport/auth combinations.
2. Fix the connection and auth lifecycle so it is protocol-driven.
   - Surface invalid JWT as `AuthenticationException`.
   - Keep the connection state machine honest from connect through close, reconnect, and shutdown.
   - Prove that closing during reconnect or active work does not revive the client.
3. Finish the public domain surface where the Fitz contract still cannot be expressed.
   - Prioritize RPC, Queue, Notice, Schedule, Stream, then KV parity gaps.
   - Add only the minimum API needed to represent the canonical contract.
   - Keep the existing API shape stable unless the contract forces a change.
4. Close the remaining transport, timeout, cancellation, and concurrency gaps.
   - Exercise both WebSocket and TCP equally.
   - Prove disconnect, reconnect, shutdown, and concurrent in-flight behavior with broker-backed tests.
   - Make response correlation and handler dispatch behavior explicit and well-tested.
5. Align reporting, docs, and tests with the shared contract.
   - Keep the conformance result schema aligned with the shared runner.
   - Refresh the parity audit and gap matrix as work lands.
   - Add or update tests for every behavior you change.

## Concrete Gap Checklist

- `docs/spec-parity-audit.md`: P0 conformance runner, auth failure handling, domain surface incompleteness, TCP coverage, reconnect coverage, concurrency/correlation, and reporting shape.
- `docs/spec-parity-gap-matrix.md`: the remaining `contract-drift`, `missing`, and `partially implemented` rows should be driven to green or explicitly justified.
- `tests/Core/Integration/ConformanceSmokeTests.cs`: replace the smoke-only runner with a full shared-suite runner.
- `tests/Core/Integration/ConformanceModels.cs`: keep the output schema aligned with the shared contract.
- `src/Core/Connection/Multiplexer.cs`: make sure same-type concurrency and request correlation match the Fitz contract, not just the current implementation convenience.

## Definition Of Done

- `dotnet test` passes for unit, integration, and conformance coverage.
- The conformance runner covers both transports and both auth modes.
- No remaining P0 gap exists in the local parity docs.
- The public API, docs, and test suite all describe the same behavior.

## Constraints

- Do not reintroduce client-side route parsing or normalization.
- Do not invent new abstractions unless the Fitz contract requires them.
- Prefer additive, non-breaking changes.
- Keep the docs honest: if behavior is partial, say so until it is proven.