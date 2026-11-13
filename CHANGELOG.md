# Changelog

## 4.1.2 (unreleased)

- Decodes Queue availability notifications using the broker's three fixed-width counters; `Payload` contains the 24 counter bytes in wire order.
- Buffers early notifications while the first subscription installs its routing state across KV, Queue, Stream, Notice, Schedule, and Lease.
- Faults subscription handles with `SubscriptionBackpressureException` and resets the connection when the shared notification queue overflows. Applications must create new subscriptions after that terminal delivery failure.
- Prevents notifications queued by an old transport session from being dispatched after reconnect.

## 4.1.1 (unreleased)

- Enables .NET 11 runtime-async compilation across projects.

## 4.1.0 (unreleased)

- Targets .NET 11 and adopts `Fitz.slnx`.
- Updates NuGet dependencies and applies Cntryl conventions across all projects.
- Centralizes project settings and streamlines documentation.
- Requires Fitz 0.4.0 / Wire 3, including negotiated Queue TTL and NACK support.

See [Wire 3 migration](docs/wire3-migration.md) for broker compatibility and Queue behavior.
Earlier release notes are available in [Git history](https://github.com/cntryl/fitz-dotnet/commits/main/CHANGELOG.md).
