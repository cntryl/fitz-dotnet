# fitz-dotnet documentation

Standing contracts — these define rules that current and future work must keep true:

- [aot-and-reflection.md](aot-and-reflection.md) — the trimming and Native AOT guarantee,
  how CI enforces it whole-program, and the rules for keeping it true.
- [../PERF_GUIDELINES.md](../PERF_GUIDELINES.md) — mandatory performance patterns,
  latency and allocation budgets, and the review checklist.
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — enforced standards, test layout, and where the
  normative client protocol specification lives.

Current status and evidence:

- [spec-parity-gap-matrix.md](spec-parity-gap-matrix.md) — capability-by-capability status
  against the shared client suite.
- [sharp-edges-evidence-ledger.md](sharp-edges-evidence-ledger.md) — disposition for all 80
  sharp-edges findings, including the tradeoffs accepted by design and the ones that cannot
  be fixed client-side without a wire-protocol change.
- [../conformance/cross-language-conformance-suite.yaml](../conformance/cross-language-conformance-suite.yaml)
  and [../compose.yml](../compose.yml) — the shared suite and the broker baseline CI runs it against.

Release history, including every breaking change and the API shape of each preview, is in
[../CHANGELOG.md](../CHANGELOG.md).
