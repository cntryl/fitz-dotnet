namespace Cntryl.Fitz;

/// <summary>
/// Current bounds of a stream.
/// </summary>
/// <param name="FirstOffset">Position of the oldest retained record.</param>
/// <param name="LastOffset">Position of the most recently committed record.</param>
/// <param name="RecordCount">Number of records currently retained.</param>
public sealed record StreamMetadata(ulong FirstOffset, ulong LastOffset, ulong RecordCount);
