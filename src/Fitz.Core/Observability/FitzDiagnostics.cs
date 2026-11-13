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
    internal static readonly Counter<long> NotificationsDropped = Meter.CreateCounter<long>(
        "fitz.notification.dropped",
        description: "Broker notifications discarded without reaching a subscriber, by domain and reason.");

    /// <summary>Counts a notification the client discarded instead of delivering.</summary>
    /// <param name="domain">The domain that received it, such as <c>kv</c>.</param>
    /// <param name="reason">Why it was dropped, such as <c>malformed</c>.</param>
    internal static void RecordDroppedNotification(string domain, string reason) =>
        NotificationsDropped.Add(1, new KeyValuePair<string, object?>("domain", domain), new KeyValuePair<string, object?>("reason", reason));
}
