namespace Cntryl.Fitz.Abstractions.Domains.Stream;

public enum StreamFilterClauseKind
{
    Equals = 0,
    NotEquals = 1,
    StartsWith = 2,
    AnyOf = 3,
}