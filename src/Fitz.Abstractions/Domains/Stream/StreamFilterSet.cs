namespace Cntryl.Fitz.Abstractions.Domains.Stream;

/// <summary>
/// A set of filter clauses applied server-side to a stream read. All clauses must match.
/// </summary>
public sealed record StreamFilterSet
{
    /// <summary>
    /// The clauses to apply. An empty set filters nothing.
    /// </summary>
    public IReadOnlyList<StreamFilterClause> Clauses { get; init; } = Array.Empty<StreamFilterClause>();
}
