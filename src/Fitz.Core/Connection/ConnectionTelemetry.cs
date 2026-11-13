using System.Globalization;
using Cntryl.Fitz.Observability;

namespace Cntryl.Fitz.Connection;

/// <summary>
/// Reports what a connection does to the application's observability hooks (logger, meter,
/// tracer, lifecycle callback) and to the built-in <see cref="FitzDiagnostics"/> instruments.
/// </summary>
/// <remarks>
/// Every hook is optional, and a failing hook never affects the connection: calls go through
/// <see cref="ObservabilityHook.Invoke{TState}(TState, Action{TState})"/>.
/// </remarks>
sealed class ConnectionTelemetry
{
    readonly ClientConfig _config;
    readonly Func<ConnectionState> _state;
    readonly Func<ITransport?> _transport;

    /// <summary>Creates the reporter for one connection.</summary>
    /// <param name="config">The connection's configuration, which carries the hooks.</param>
    /// <param name="state">Reads the connection's current state for lifecycle events.</param>
    /// <param name="transport">Reads the connection's current transport for lifecycle events.</param>
    internal ConnectionTelemetry(ClientConfig config, Func<ConnectionState> state, Func<ITransport?> transport)
    {
        _config = config;
        _state = state;
        _transport = transport;
    }

    // Enum.ToString resolves names through runtime metadata; an explicit map keeps the
    // enum name tables trimmable.
    internal static string Describe(ConnectionState state) => state switch
    {
        ConnectionState.Disconnected => nameof(ConnectionState.Disconnected),
        ConnectionState.Connecting => nameof(ConnectionState.Connecting),
        ConnectionState.Connected => nameof(ConnectionState.Connected),
        ConnectionState.Reconnecting => nameof(ConnectionState.Reconnecting),
        ConnectionState.Authenticating => nameof(ConnectionState.Authenticating),
        ConnectionState.Authenticated => nameof(ConnectionState.Authenticated),
        ConnectionState.Closed => nameof(ConnectionState.Closed),
        _ => ((int)state).ToString(CultureInfo.InvariantCulture),
    };

    internal void Log(FitzLogLevel level, string eventName, IReadOnlyDictionary<string, object?>? fields = null)
    {
        if (_config.Observability?.Logger is { } logger)
            ObservabilityHook.Invoke((logger, level, eventName, fields), static s => s.logger.Log(s.level, s.eventName, s.fields));
    }

    internal void LifecycleEvent(string eventName, Exception? exception = null, int? attempt = null, ushort? code = null, uint? graceMs = null)
    {
        if (_config.Observability?.OnLifecycleEvent is not { } onLifecycleEvent)
            return;

        var transport = _transport();
        var lifecycleEvent = new FitzLifecycleEvent(
            eventName,
            _state(),
            transport?.TransportName,
            transport?.Url ?? _config.Url,
            attempt,
            exception?.Message,
            code,
            graceMs);
        ObservabilityHook.Invoke((onLifecycleEvent, lifecycleEvent), static s => s.onLifecycleEvent(s.lifecycleEvent));
    }

    internal IFitzSpan? StartSpan(string name, ITransport transport)
    {
        if (_config.Observability?.Tracer is not { } tracer)
            return null;

        return ObservabilityHook.Invoke((tracer, name, transport), static s => s.tracer.StartSpan(s.name, new Dictionary<string, object?>
        {
            ["server.address"] = s.transport.Url.Host,
            ["network.transport"] = s.transport.TransportName,
        }));
    }

    internal static void RecordSpanException(IFitzSpan? span, Exception exception)
    {
        if (span is not null)
            ObservabilityHook.Invoke((span, exception), static s => s.span.RecordException(s.exception));
    }

    internal static void FinishSpan(IFitzSpan? span)
    {
        if (span is not null)
            ObservabilityHook.Invoke(span, static s => s.Finish());
    }

    internal void HandlerFailed(Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.connection.handler_failed", new Dictionary<string, object?>
        {
            ["error"] = exception.Message,
        });
    }

    internal void HandlerMetricsChanged(int activeCount, int queuedCount)
    {
        Gauge("fitz.async_handlers.active", activeCount);
        Gauge("fitz.async_handlers.queued", queuedCount);
    }

    internal void HandlerSaturated(int activeCount, int queuedCount)
    {
        Log(FitzLogLevel.Warn, "fitz.connection.handler_saturated", new Dictionary<string, object?>
        {
            ["activeCount"] = activeCount,
            ["queuedCount"] = queuedCount,
        });
        Counter("fitz.async_handlers.saturated");
        FitzDiagnostics.HandlerSaturated.Add(1);
    }

    internal void RequestQueueFull()
    {
        Log(FitzLogLevel.Warn, "fitz.request_gate.full", new Dictionary<string, object?>
        {
            ["maxInFlightRequests"] = _config.MaxInFlightRequests,
            ["maxRequestQueueSize"] = _config.ResolvedMaxRequestQueueSize,
        });
        Counter("fitz.request_gate.full");
        FitzDiagnostics.RequestQueueFull.Add(1);
    }

    internal void RequestTimedOut(ushort messageType, TimeSpan timeout)
    {
        Log(FitzLogLevel.Warn, "fitz.request.timeout", new Dictionary<string, object?>
        {
            ["messageType"] = messageType,
            ["timeoutMs"] = timeout.TotalMilliseconds,
        });
        Counter("fitz.request.timeout");
        FitzDiagnostics.RequestTimeouts.Add(1);
    }

    internal void Retrying(RetryOperation operation, int attempt, TimeSpan delay, Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.request.retry", new Dictionary<string, object?>
        {
            ["domain"] = operation.Domain,
            ["operation"] = operation.Operation,
            ["attempt"] = attempt,
            ["delayMs"] = delay.TotalMilliseconds,
            ["error"] = exception.Message,
        });
        Counter("fitz.request.retry");
        FitzDiagnostics.RequestRetries.Add(1);
    }

    internal void RetryExhausted(RetryOperation operation, int attempt, Exception exception)
    {
        Log(FitzLogLevel.Warn, "fitz.request.retry_exhausted", new Dictionary<string, object?>
        {
            ["domain"] = operation.Domain,
            ["operation"] = operation.Operation,
            ["attempt"] = attempt,
            ["error"] = exception.Message,
        });
        Counter("fitz.request.retry_exhausted");
        FitzDiagnostics.RetryExhausted.Add(1);
    }

    void Counter(string name)
    {
        if (_config.Observability?.Meter is { } meter)
            ObservabilityHook.Invoke((meter, name), static s => s.meter.Counter(s.name, 1));
    }

    void Gauge(string name, double value)
    {
        if (_config.Observability?.Meter is { } meter)
            ObservabilityHook.Invoke((meter, name, value), static s => s.meter.Gauge(s.name, s.value));
    }
}
