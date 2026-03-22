namespace Cntryl.Fitz.Abstractions.Domains.Notice;

/// <summary>
/// Notice message received from a published route.
/// Received via Subscribe on notice patterns.
/// </summary>
public sealed record NoticeMessage(string Route, ReadOnlyMemory<byte> Body);
