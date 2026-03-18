namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// Stream session commit notification.
/// Sent when a session commits records to the stream.
/// </summary>
public sealed record StreamCommitEvent(string Route, ulong CommitOffset);
