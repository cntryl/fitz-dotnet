namespace Cntryl.Fitz;

/// <summary>
/// One server-side filter condition applied to a stream read.
/// </summary>
public sealed record StreamFilterClause
{
    /// <summary>How the clause compares. Determines whether it reads <see cref="Value"/> or <see cref="Values"/>.</summary>
    public StreamFilterClauseKind Kind { get; init; }

    /// <summary>
    /// Operand for <see cref="StreamFilterClauseKind.Equals"/>,
    /// <see cref="StreamFilterClauseKind.NotEquals"/>, and
    /// <see cref="StreamFilterClauseKind.StartsWith"/>.
    /// </summary>
    public string? Value { get; init; }

    /// <summary>Operands for <see cref="StreamFilterClauseKind.AnyOf"/>.</summary>
    public IReadOnlyList<string> Values { get; init; } = Array.Empty<string>();
}
