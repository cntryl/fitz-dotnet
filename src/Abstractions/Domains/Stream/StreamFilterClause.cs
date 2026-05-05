namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public sealed record StreamFilterClause
{
    public StreamFilterClauseKind Kind { get; init; }
    public string? Value { get; init; }
    public IReadOnlyList<string> Values { get; init; } = Array.Empty<string>();
}