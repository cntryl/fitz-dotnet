namespace Cntryl.Fitz.Extensions;

/// <summary>One committed batch of an index-generation backfill.</summary>
/// <param name="Processed">Number of primary records indexed by this batch.</param>
/// <param name="NextCursor">Opaque primary-key continuation, or null when backfill is complete.</param>
public sealed record KvDirectoryBackfillPage(int Processed, string? NextCursor);
