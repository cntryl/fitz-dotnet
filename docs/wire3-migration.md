# Wire 3 migration

Fitz .NET 4.0.0 requires a Fitz 0.4.0 broker that supports Wire 3. Upgrade the broker and
client fleet together; Wire 1 and Wire 2 fallback is not supported.

## Queue changes

Capability 1 enables a positive per-message TTL in hours. Set it with
`EnqueueWithOptionsAsync()` and `QueueEnqueueOptions(TtlHours: hours)`. TTL begins at
publication and includes any initial delay. NACK, reservation, restart, and replay do not
restart the TTL; an omitted TTL uses the broker default.

Capability 2 enables NACK with a nonnegative visibility delay in seconds. Call
`item.NackAsync(visibilityDelay)`. A successful NACK closes the local reservation. Broker
NACK deduplication is ephemeral, does not survive restart, and does not change the item body
or attempt count. Another RESERVE increments attempts.

## Negotiation and session behavior

CONNECT and HELLO use version 3 and negotiate an explicit numeric capability list. Capabilities
belong to the current session and are negotiated again after reconnect. Requests for optional
semantics fail if the broker did not negotiate the required capability.

Disconnect ends session-owned handles, tokens, leases, and pending requests. Rebuild
registrations for the new session. Wire 3 does not provide replay, exactly-once delivery,
durable downstream Schedule delivery, or ownership continuity. Queue persistence remains
best effort.

See the [Wire 3 contract](https://github.com/cntryl/fitz/blob/main/docs/clients/spec/wire-routing-3.md)
and [extension registry](https://github.com/cntryl/fitz/blob/main/docs/clients/spec/extension-registry.md)
for the normative wire details.
