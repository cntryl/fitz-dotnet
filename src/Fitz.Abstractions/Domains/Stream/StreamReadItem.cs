namespace Cntryl.Fitz;

/// <summary>
/// One entry in a stream read page: either a delivered record or a marker for content the
/// broker withheld.
/// </summary>
/// <param name="Route">The concrete route this entry belongs to, never a request pattern.</param>
/// <param name="Kind">Which of the remaining fields carry meaning.</param>
/// <param name="Record">The record, when <paramref name="Kind"/> is <see cref="StreamReadItemKind.Event"/>.</param>
/// <param name="Offset">
/// Position of the withheld record, when <paramref name="Kind"/> is
/// <see cref="StreamReadItemKind.Filtered"/>.
/// </param>
/// <param name="FromOffset">
/// First position of a withheld range, when <paramref name="Kind"/> is
/// <see cref="StreamReadItemKind.FilteredRange"/>.
/// </param>
/// <param name="ToOffset">
/// Last position of a withheld range, when <paramref name="Kind"/> is
/// <see cref="StreamReadItemKind.FilteredRange"/>.
/// </param>
/// <param name="Reason">Why the content was withheld, for the filtered kinds.</param>
/// <remarks>
/// Filtered entries preserve sequence continuity: a gap in positions is reported explicitly
/// rather than leaving the caller to infer it.
/// </remarks>
public sealed record StreamReadItem(
    string Route,
    StreamReadItemKind Kind,
    StreamRecord? Record = null,
    ulong Offset = 0,
    ulong FromOffset = 0,
    ulong ToOffset = 0,
    StreamFilteredReason? Reason = null);
