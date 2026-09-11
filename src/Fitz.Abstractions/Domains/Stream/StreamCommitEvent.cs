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
    /// <summary>The metadata carried a valid commit offset.</summary>
    Present,

    /// <summary>The metadata carried no commit offset.</summary>
    Absent,

    /// <summary>The metadata carried a commit offset that could not be parsed.</summary>
    Malformed,
}
