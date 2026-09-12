namespace Cntryl.Fitz;

/// <summary>
/// How a <see cref="StreamFilterClause"/> compares a record.
/// </summary>
public enum StreamFilterClauseKind
{
    /// <summary>Matches when the record equals <see cref="StreamFilterClause.Value"/>.</summary>
    Equals = 0,

    /// <summary>Matches when the record differs from <see cref="StreamFilterClause.Value"/>.</summary>
    NotEquals = 1,

    /// <summary>Matches when the record starts with <see cref="StreamFilterClause.Value"/>.</summary>
    StartsWith = 2,

    /// <summary>Matches when the record equals any of <see cref="StreamFilterClause.Values"/>.</summary>
    AnyOf = 3,
}
