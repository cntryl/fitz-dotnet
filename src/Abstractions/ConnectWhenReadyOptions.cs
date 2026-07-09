namespace Cntryl.Fitz;

public sealed record ConnectWhenReadyOptions(
    TimeSpan? Timeout = null,
    TimeSpan? Backoff = null,
    TimeSpan? MaxBackoff = null
);
