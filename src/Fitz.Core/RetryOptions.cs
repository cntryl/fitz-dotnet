namespace Cntryl.Fitz;

/// <summary>
/// Controls automatic retry of individual requests that fail with a retryable error.
/// </summary>
/// <param name="Enabled">Whether to retry retryable failures. Enabled by default.</param>
/// <param name="MaxAttempts">Maximum attempts per request, including the first.</param>
/// <param name="Backoff">Delay before the first retry. Subsequent delays grow from it.</param>
/// <param name="MaxBackoff">Ceiling applied to the growing delay.</param>
/// <remarks>
/// Only failures classified retryable by <c>Retryability</c> are retried, and only for
/// operations safe to repeat. Terminal operations are never retried after broker success.
/// </remarks>
public sealed record RetryOptions(
    bool Enabled = true,
    int MaxAttempts = 3,
    TimeSpan? Backoff = null,
    TimeSpan? MaxBackoff = null
);
