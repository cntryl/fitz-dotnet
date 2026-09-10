namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// Stream session commit notification.
/// Sent when a session commits records to the stream.
/// </summary>
public sealed record StreamCommitEvent(string Route, ulong CommitOffset)
{
    /// <summary>Whether the broker metadata contained a valid commit offset.</summary>
    public StreamCommitOffsetStatus CommitOffsetStatus { get; init; } = StreamCommitOffsetStatus.Present;
}

/// <summary>Describes how a stream commit offset was obtained from broker metadata.</summary>
public enum StreamCommitOffsetStatus
{
    Present,
    Absent,
    Malformed,
}
