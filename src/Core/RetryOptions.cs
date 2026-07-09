namespace Cntryl.Fitz;

public sealed record RetryOptions(
    bool Enabled = true,
    int MaxAttempts = 3,
    TimeSpan? Backoff = null,
    TimeSpan? MaxBackoff = null
);
