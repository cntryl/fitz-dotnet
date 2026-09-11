namespace Cntryl.Fitz;

/// <summary>
/// Controls bounded asynchronous callback execution.
/// </summary>
/// <param name="MaxConcurrency">
/// Maximum callbacks running at once per domain dispatcher, or <see langword="null"/> for
/// the default. Must be positive when set.
/// </param>
/// <param name="Timeout">
/// Deadline for a single callback. <see langword="null"/> means callbacks have no deadline;
/// configure a finite value only when callback cancellation is an explicit application policy.
/// </param>
/// <param name="QueueCapacity">
/// Depth of the callback delivery queue. Overflow faults the affected subscription's
/// <c>Completion</c> with <c>AsyncHandlerOverflowException</c> and ends its registration.
/// </param>
/// <param name="SubscriptionBufferCapacity">
/// Per-subscription notification buffer depth. Overflow terminates that handle with
/// <c>SubscriptionBackpressureException</c> without affecting sibling handles.
/// </param>
/// <remarks>
/// Dispatch is bounded per domain rather than client-wide: each lazily created domain
/// dispatcher gets its own concurrency and queue capacity.
/// </remarks>
public sealed record AsyncHandlerOptions(
    int? MaxConcurrency = null,
    TimeSpan? Timeout = null,
    int QueueCapacity = 1024,
    int SubscriptionBufferCapacity = 256
);
