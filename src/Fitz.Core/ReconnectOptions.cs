namespace Cntryl.Fitz;

public sealed record ReconnectOptions(
    bool Enabled = true,
    int MaxAttempts = int.MaxValue,
    TimeSpan? Backoff = null,
    TimeSpan? MaxBackoff = null
);
