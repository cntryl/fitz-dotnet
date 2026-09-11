namespace Cntryl.Fitz;

/// <summary>
/// Controls bounded asynchronous callback execution. A null <paramref name="Timeout"/>
/// means callbacks have no deadline; configure a finite value only when callback
/// cancellation is an explicit application policy.
/// </summary>
public sealed record AsyncHandlerOptions(
    int? MaxConcurrency = null,
    TimeSpan? Timeout = null,
    int QueueCapacity = 1024,
    int SubscriptionBufferCapacity = 256
);
