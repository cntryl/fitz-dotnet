namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamFilterSet
{
    public IReadOnlyList<StreamFilterClause> Clauses { get; init; } = Array.Empty<StreamFilterClause>();
}