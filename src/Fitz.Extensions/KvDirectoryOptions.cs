namespace Cntryl.Fitz.Extensions;

/// <summary>Bounds directory query and cursor resources.</summary>
public sealed record KvDirectoryOptions
{
    /// <summary>Gets the page size used when a query does not choose one.</summary>
    public int DefaultPageSize { get; init; } = 50;

    /// <summary>Gets the largest page or backfill batch the directory accepts.</summary>
    public int MaximumPageSize { get; init; } = 200;

    /// <summary>Gets the largest encoded cursor the directory accepts.</summary>
    public int MaximumCursorBytes { get; init; } = 2048;

    /// <summary>Gets the largest serialized entity accepted by a write.</summary>
    public int MaximumSerializedValueBytes { get; init; } = 1024 * 1024;

    /// <summary>Gets the largest number of simultaneously maintained index generations.</summary>
    public int MaximumIndexGenerations { get; init; } = 32;

    /// <summary>Gets the largest number of rows one index generation may produce for one entity.</summary>
    public int MaximumIndexEntriesPerEntity { get; init; } = 128;
}
