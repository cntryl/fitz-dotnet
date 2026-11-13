; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### Changed Rules

Rule ID | New Category | New Severity | Old Category | Old Severity | Notes
--------|--------------|--------------|--------------|--------------|------
FITZ001 | Usage | Error | Usage | Warning | An invalid route literal is rejected by the client at runtime, so it now fails the build.
FITZ002 | Usage | Error | Usage | Warning | An invalid pattern literal is rejected by the client at runtime, so it now fails the build.
FITZ003 | Reliability | Error | Reliability | Warning | A discarded lease, transaction, or subscription handle leaks the resource it owns, so it now fails the build.
FITZ004 | Usage | Error | Usage | Warning | An invalid argument or duration literal is rejected by the client at runtime, so it now fails the build.
