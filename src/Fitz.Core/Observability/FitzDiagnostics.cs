using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Cntryl.Fitz.Observability;

static class FitzDiagnostics
{
    internal const string InstrumentationName = "Cntryl.Fitz";
    internal static readonly ActivitySource ActivitySource = new(InstrumentationName);
    internal static readonly Meter Meter = new(InstrumentationName);
    internal static readonly Counter<long> RequestTimeouts = Meter.CreateCounter<long>("fitz.request.timeout");
    internal static readonly Counter<long> RequestQueueFull = Meter.CreateCounter<long>("fitz.request_gate.full");
    internal static readonly Counter<long> RequestRetries = Meter.CreateCounter<long>("fitz.request.retry");
    internal static readonly Counter<long> RetryExhausted = Meter.CreateCounter<long>("fitz.request.retry_exhausted");
    internal static readonly Counter<long> HandlerSaturated = Meter.CreateCounter<long>("fitz.async_handlers.saturated");
}
