# 4.0.0: Fitz 0.4.0 / Wire 3

This SDK requires the coordinated Fitz 0.4.0 broker candidate. Wire 1 and Wire 2 fallback is not supported. Upgrade or roll back broker and SDKs together.

The 15-byte header, message IDs, core payloads, correlations, and terminal-response flag retain their meanings. Bit 2 marks an extension envelope: `[u32 core_length][core][u16 field_id][u32 length][value]...`. Unknown fields are bounded and skipped before dispatch. Malformed fields and duplicate known singletons fail the session.

CONNECT and HELLO use version 3, zero legacy capability bits, and one explicit numeric capability-list extension, including an empty list. Capabilities belong to the current session and are negotiated again on reconnect. Capability 1 permits a positive per-message Queue TTL in **hours**. Capability 2 permits NACK with a nonnegative visibility delay in **seconds**. Requested optional semantics fail clearly if support was not negotiated.

Use `EnqueueWithOptionsAsync()` with `QueueEnqueueOptions(TtlHours: hours)` and `item.NackAsync(visibilityDelay)`.

Successful NACK closes the local reservation. Its broker deduplication is ephemeral and does not survive restart. NACK preserves the body and attempt count; only another RESERVE increments attempts. TTL starts at publication and includes the initial delay; NACK, reservation, restart, and replay do not restart it. An omitted TTL uses the broker default. No attempt count or TTL metadata was added to RESERVE.

Queue persistence remains best effort. Disconnect destroys session-owned handles, tokens, leases, and pending requests. Explicitly rebuilt registrations form a new session. Wire 3 does not add replay, exactly-once delivery, durable downstream Schedule delivery, or ownership continuity.

The broker contract is [Wire 3](https://github.com/cntryl/fitz/blob/main/docs/clients/spec/wire-routing-3.md), with assignments in the [extension registry](https://github.com/cntryl/fitz/blob/main/docs/clients/spec/extension-registry.md). Shared golden bytes are copied from the broker's `tests/fixtures/golden/wire3_vectors.txt`.

This change prepares source and qualification. Package publication and release tags are separate explicit operations.
