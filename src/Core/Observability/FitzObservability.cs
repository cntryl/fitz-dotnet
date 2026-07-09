namespace Cntryl.Fitz.Observability;

using Cntryl.Fitz;

public enum FitzLogLevel
{
    Debug,
    Info,
    Warn,
    Error,
}

public interface IFitzLogger
{
    void Log(FitzLogLevel level, string eventName, IReadOnlyDictionary<string, object?>? fields = null);
}

public interface IFitzSpan
{
    void SetAttribute(string key, object? value);
    void RecordException(Exception exception);
    void End();
}

public interface IFitzTracer
{
    IFitzSpan StartSpan(string name, IReadOnlyDictionary<string, object?>? attributes = null);
}

public interface IFitzMeter
{
    void Counter(string name, long value, IReadOnlyDictionary<string, object?>? attributes = null);
    void Histogram(string name, double value, IReadOnlyDictionary<string, object?>? attributes = null);
    void Gauge(string name, double value, IReadOnlyDictionary<string, object?>? attributes = null);
}

public sealed record FitzLifecycleEvent(
    string Event,
    ConnectionState State,
    string? Transport = null,
    string? Url = null,
    int? Attempt = null,
    string? Error = null
);

public sealed record FitzObservabilityOptions(
    IFitzLogger? Logger = null,
    IFitzTracer? Tracer = null,
    IFitzMeter? Meter = null,
    Action<FitzLifecycleEvent>? OnLifecycleEvent = null
);
