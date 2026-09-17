namespace Cntryl.Fitz.Extensions;

/// <summary>
/// A normalized list query, built once per request via <see cref="From"/> so directory readers
/// take one object instead of a parameter per capability (limit, cursor, search, sort, ...).
/// </summary>
/// <param name="Limit">The clamped page size.</param>
/// <param name="Cursor">The opaque cursor resuming a previous page, or <see langword="null"/> for the first page.</param>
/// <param name="Search">The normalized free-text filter, or <see langword="null"/> for no filter.</param>
/// <param name="Sort">The requested sort order, in request order; empty for the directory's natural key order.</param>
public sealed record ListQuery(int Limit, string? Cursor, string? Search, IReadOnlyList<SortField> Sort)
{
    /// <summary>The page size used when a caller requests none.</summary>
    public const int DefaultLimit = 50;

    /// <summary>The largest page size a caller may request.</summary>
    public const int MaxLimit = 200;

    /// <summary>
    /// Builds a normalized query from raw, caller-supplied values: clamps <paramref name="limit"/>
    /// to <c>[1, maxLimit]</c> (default <see cref="DefaultLimit"/>), trims <paramref name="search"/>
    /// and treats whitespace as no filter, and parses <paramref name="sort"/> via <see cref="SortField.Parse"/>.
    /// </summary>
    /// <param name="limit">The requested page size, or <see langword="null"/> for the default.</param>
    /// <param name="cursor">The opaque cursor from a previous page, or <see langword="null"/> for the first page.</param>
    /// <param name="search">The raw free-text filter, or <see langword="null"/>/whitespace for none.</param>
    /// <param name="sort">The raw <c>field[:asc|desc]</c> sort expression, or <see langword="null"/> for natural key order.</param>
    /// <param name="defaultLimit">The page size used when <paramref name="limit"/> is <see langword="null"/>.</param>
    /// <param name="maxLimit">The largest page size <paramref name="limit"/> may clamp to.</param>
    /// <returns>The normalized query.</returns>
    public static ListQuery From(
        int? limit,
        string? cursor,
        string? search,
        string? sort,
        int defaultLimit = DefaultLimit,
        int maxLimit = MaxLimit) =>
        new(
            Math.Clamp(limit ?? defaultLimit, 1, maxLimit),
            cursor,
            string.IsNullOrWhiteSpace(search) ? null : search.Trim(),
            SortField.Parse(sort));
}
