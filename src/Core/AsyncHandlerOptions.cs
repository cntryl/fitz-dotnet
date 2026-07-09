namespace Cntryl.Fitz;

public sealed record AsyncHandlerOptions(
    int? MaxConcurrency = null,
    TimeSpan? Timeout = null
);
