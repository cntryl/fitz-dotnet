# Stream error envelope generation 2 (Fitz #238)

This client decodes Stream status 2 as `[u32 BE domain_code][string message]`.
Legacy status 1 remains supported: READ includes a code; other operations carry
text only. Unknown codes remain available to callers. Classify OCC using 2001,
never message wording; backend failures use 2012. No automatic command retry is
introduced. Success and notification layouts remain unchanged.

Deploy the updated .NET, TypeScript, Go, Python, and Rust clients before the
broker that emits status 2. Old clients cannot decode the new envelope. For
rollback, restore the broker first and retain these dual-generation clients.
Record exact published versions at release; the issue-238 working branches and
local qualification packages are not published releases.

The .NET client exposes `StreamException.DomainCode`, including 2001 for APPEND
and COMMIT. Legacy uncoded failures remain null. Go preserves the domain error
alongside its sentinel via `errors.Join`; use `errors.Is` and `errors.As`.
