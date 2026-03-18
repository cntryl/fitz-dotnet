namespace Cntryl.Fitz;

public sealed record ReconnectOptions(
    bool Enabled = false,
    int MaxAttempts = int.MaxValue,
    TimeSpan? Backoff = null,
    TimeSpan? MaxBackoff = null
);
