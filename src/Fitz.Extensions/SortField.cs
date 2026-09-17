namespace Cntryl.Fitz.Extensions;

/// <summary>One column of a list query's requested sort order.</summary>
/// <param name="Field">The field name, matched by a directory's configured sort selectors.</param>
/// <param name="Descending">Whether this field sorts descending; ascending when <see langword="false"/>.</param>
public sealed record SortField(string Field, bool Descending = false)
{
    /// <summary>Parses a comma-separated <c>field[:asc|desc]</c> list, e.g. <c>"name:desc,id"</c>.</summary>
    /// <param name="sort">The raw sort expression, or <see langword="null"/>/whitespace for no sort.</param>
    /// <returns>The parsed fields in request order, or an empty list when <paramref name="sort"/> requests none.</returns>
    public static IReadOnlyList<SortField> Parse(string? sort)
    {
        if (string.IsNullOrWhiteSpace(sort))
        {
            return [];
        }

        return [.. sort
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static term =>
            {
                var parts = term.Split(':', 2, StringSplitOptions.TrimEntries);
                var descending = parts.Length == 2 &&
                    string.Equals(parts[1], "desc", StringComparison.OrdinalIgnoreCase);
                return new SortField(parts[0], descending);
            })];
    }
}
