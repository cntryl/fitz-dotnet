using Cntryl.Keys;

namespace Cntryl.Fitz.Extensions;

/// <summary>An immutable, index-backed directory query.</summary>
public sealed record KvDirectoryQuery<T>
{
    internal KvDirectoryQuery(KvDirectoryIndex<T> index)
    {
        Index = index;
    }

    internal KvDirectoryIndex<T> Index { get; init; }
    internal LexKeyPart[] Prefix { get; init; } = [];
    internal bool IsDescending { get; init; }
    internal int? Limit { get; init; }
    internal string? Cursor { get; init; }

    /// <summary>Restricts the scan to an index-key prefix.</summary>
    public KvDirectoryQuery<T> WithPrefix(params LexKeyPart[] prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        return this with { Prefix = [.. prefix] };
    }

    /// <summary>Scans this index generation in descending byte order.</summary>
    public KvDirectoryQuery<T> Descending() => this with { IsDescending = true };

    /// <summary>Sets the requested page size.</summary>
    public KvDirectoryQuery<T> Take(int limit) => this with { Limit = limit };

    /// <summary>Resumes exclusively after a cursor produced by the same query.</summary>
    public KvDirectoryQuery<T> After(string? cursor) => this with { Cursor = cursor };
}
