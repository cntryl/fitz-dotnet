using Cntryl.Fitz;

namespace Cntryl.Fitz.Observability;

/// <summary>
/// Severity of a client log record.
/// </summary>
public enum FitzLogLevel
{
    /// <summary>Detailed diagnostics, off in normal operation.</summary>
    Debug,

    /// <summary>Routine lifecycle progress.</summary>
    Info,

    /// <summary>A recoverable problem, such as a retried request.</summary>
    Warn,

    /// <summary>A failure the client could not recover from.</summary>
    Error,
}

/// <summary>
/// Receives structured log records from the client.
/// </summary>
/// <remarks>
/// Implementations must not throw and must not block; the client guards every call and
/// discards failures so observability never alters connection behavior.
/// </remarks>
public interface IFitzLogger
{
    /// <summary>
    /// Records one log event.
    /// </summary>
    /// <param name="level">Severity of the event.</param>
    /// <param name="eventName">Stable event identifier, such as <c>connect_succeeded</c>.</param>
    /// <param name="fields">Structured fields describing the event, if any.</param>
    void Log(FitzLogLevel level, string eventName, IReadOnlyDictionary<string, object?>? fields = null);
}

/// <summary>
/// A single tracing span started by the client. Always finished exactly once.
/// </summary>
public interface IFitzSpan
{
    /// <summary>
    /// Attaches an attribute to the span.
    /// </summary>
    /// <param name="key">Attribute name.</param>
    /// <param name="value">Attribute value.</param>
    void SetAttribute(string key, object? value);

    /// <summary>
    /// Records an exception against the span.
    /// </summary>
    /// <param name="exception">The failure to record.</param>
    void RecordException(Exception exception);

    /// <summary>Ends the span.</summary>
    void Finish();
}

/// <summary>
/// Creates tracing spans for client operations.
/// </summary>
/// <remarks>
/// A built-in <see cref="System.Diagnostics.ActivitySource"/> bridge is always active; this
/// hook is for exporters that do not consume <c>Activity</c>. Implementations must not throw.
/// </remarks>
public interface IFitzTracer
{
    /// <summary>
    /// Starts a span.
    /// </summary>
    /// <param name="name">Operation name, such as <c>fitz.connect</c>.</param>
    /// <param name="attributes">Attributes known when the span starts.</param>
    /// <returns>The started span.</returns>
    IFitzSpan StartSpan(string name, IReadOnlyDictionary<string, object?>? attributes = null);
}

/// <summary>
/// Receives client metrics.
/// </summary>
/// <remarks>
/// A built-in <see cref="System.Diagnostics.Metrics.Meter"/> bridge is always active; this
/// hook is for exporters that do not consume it. Implementations must not throw.
/// </remarks>
public interface IFitzMeter
{
    /// <summary>
    /// Adds to a monotonically increasing counter.
    /// </summary>
    /// <param name="name">Instrument name.</param>
    /// <param name="value">Amount to add.</param>
    /// <param name="attributes">Dimensions for this measurement.</param>
    void Counter(string name, long value, IReadOnlyDictionary<string, object?>? attributes = null);

    /// <summary>
    /// Records a value into a distribution.
    /// </summary>
    /// <param name="name">Instrument name.</param>
    /// <param name="value">Value to record.</param>
    /// <param name="attributes">Dimensions for this measurement.</param>
    void Histogram(string name, double value, IReadOnlyDictionary<string, object?>? attributes = null);

    /// <summary>
    /// Records the current value of a gauge.
    /// </summary>
    /// <param name="name">Instrument name.</param>
    /// <param name="value">Current value.</param>
    /// <param name="attributes">Dimensions for this measurement.</param>
    void Gauge(string name, double value, IReadOnlyDictionary<string, object?>? attributes = null);
}

/// <summary>
/// A connection lifecycle transition reported to <see cref="FitzObservabilityOptions.OnLifecycleEvent"/>.
/// </summary>
/// <param name="Event">
/// Stable event name, such as <c>connect_start</c>, <c>connect_succeeded</c>,
/// <c>auth_rejected</c>, <c>connection_lost</c>, or <c>closed</c>.
/// </param>
/// <param name="State">Connection state when the event was emitted.</param>
/// <param name="Transport">
/// The transport's <see cref="Transport.ITransport.TransportName"/>, or <see langword="null"/>
/// when no transport exists yet.
/// </param>
/// <param name="Url">Endpoint involved, falling back to the configured URL.</param>
/// <param name="Attempt">Attempt number, for retry and reconnect events.</param>
/// <param name="Error">Failure message, for events that carry one.</param>
public sealed record FitzLifecycleEvent(
    string Event,
    ConnectionState State,
    string? Transport = null,
    Uri? Url = null,
    int? Attempt = null,
    string? Error = null
);

/// <summary>
/// Observability hooks for a client.
/// </summary>
/// <param name="Logger">Receives structured log records.</param>
/// <param name="Tracer">Creates spans for client operations.</param>
/// <param name="Meter">Receives metrics.</param>
/// <param name="OnLifecycleEvent">Called on each connection lifecycle transition.</param>
/// <remarks>
/// Every hook is optional and every call is guarded: a hook that throws is swallowed and
/// never alters client behavior. Hooks run outside the client's internal locks.
/// </remarks>
public sealed record FitzObservabilityOptions(
    IFitzLogger? Logger = null,
    IFitzTracer? Tracer = null,
    IFitzMeter? Meter = null,
    Action<FitzLifecycleEvent>? OnLifecycleEvent = null
);
