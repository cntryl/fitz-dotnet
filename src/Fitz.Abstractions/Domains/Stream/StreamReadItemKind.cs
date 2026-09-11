namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// What a <see cref="StreamReadItem"/> represents.
/// </summary>
public enum StreamReadItemKind
{
    /// <summary>A delivered record, available on <see cref="StreamReadItem.Record"/>.</summary>
    Event = 0,

    /// <summary>A single withheld record at <see cref="StreamReadItem.Offset"/>.</summary>
    Filtered = 1,

    /// <summary>
    /// A withheld contiguous range, from <see cref="StreamReadItem.FromOffset"/> to
    /// <see cref="StreamReadItem.ToOffset"/>.
    /// </summary>
    FilteredRange = 2,
}
