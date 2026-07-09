namespace Cntryl.Fitz;

public sealed record HeartbeatOptions(
    bool Enabled = true,
    TimeSpan? Interval = null,
    TimeSpan? Timeout = null
);
