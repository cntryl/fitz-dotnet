# fitz-dotnet Documentation

Use the local repo docs as the current source of truth for the .NET client:

- [../README.md](../README.md)
- [../CLIENT_SPEC.md](../CLIENT_SPEC.md)
- [../CLIENT_ACCEPTANCE_CRITERIA.md](../CLIENT_ACCEPTANCE_CRITERIA.md)
- [../conformance/cross-language-conformance-suite.yaml](../conformance/cross-language-conformance-suite.yaml)
- [../compose.yml](../compose.yml)
- [spec-parity-gap-matrix.md](spec-parity-gap-matrix.md)
- [spec-parity-audit.md](spec-parity-audit.md)
- [migration.md](migration.md)
- [aot-and-reflection.md](aot-and-reflection.md)

The parity matrix tracks current status. The audit document is historical only.
`aot-and-reflection.md` is a standing contract, not a historical record: it defines
the trimming/Native AOT guarantee, how CI enforces it, and the rules contributors
must follow to keep it true.
