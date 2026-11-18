namespace Cntryl.Fitz;

/// <summary>How a stream read behaves at the end of a snapshot.</summary>
public enum StreamReadMode
{
    /// <summary>Complete after yielding the first caught-up batch.</summary>
    Replay,
    /// <summary>Subscribe before reading and resume after commits and reconnections.</summary>
    Follow,
}

/// <summary>Limits and filtering applied to each broker read.</summary>
public sealed record StreamReadOptions
{
    /// <summary>Maximum items requested per batch. Defaults to 100.</summary>
    public ulong BatchSize { get; init; } = 100;

    /// <summary>Optional server-side filter.</summary>
    public StreamFilterSet? Filter { get; init; }

    /// <summary>Optional maximum bytes per batch.</summary>
    public ulong? MaxBytes { get; init; }
}
