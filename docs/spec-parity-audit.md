# fitz-dotnet Spec/Parity Audit

Date of original audit snapshot: 2026-04-24
Status of this document: historical

This file is retained as an audit trail for the earlier parity review that predated the final runtime, conformance, and CI work.

Do not use this file as current implementation status. Current truth lives in:

- [spec-parity-gap-matrix.md](spec-parity-gap-matrix.md)
- [../README.md](../README.md)
- [../conformance/cross-language-conformance-suite.yaml](../conformance/cross-language-conformance-suite.yaml)
- [../.github/workflows/ci.yml](../.github/workflows/ci.yml)

The April 2026 audit identified gaps that have since been closed, including:

- incomplete shared-suite coverage
- stale conformance artifact shape
- startup auth/probe drift
- same-client reconnect proof
- missing repo-owned broker baseline
- stale documentation around suite size and runtime behavior

If a fresh audit is needed, start from the current shared 17-scenario suite and the broker-backed CI artifacts rather than from the superseded findings that were once captured here.
