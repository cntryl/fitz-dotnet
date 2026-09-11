using Microsoft.CodeAnalysis;

namespace Cntryl.Fitz.Analyzers;

public static class FitzDiagnostics
{
    public const string InvalidRouteId = "FITZ001";
    public const string InvalidPatternId = "FITZ002";
    public const string DiscardedHandleId = "FITZ003";
    public const string InvalidArgumentId = "FITZ004";

    internal static readonly DiagnosticDescriptor InvalidRoute = new(
        InvalidRouteId,
        "Invalid Fitz route",
        "'{0}' is not a valid {1} route",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor InvalidPattern = new(
        InvalidPatternId,
        "Invalid Fitz pattern",
        "'{0}' is not a valid {1} pattern or selector",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor DiscardedHandle = new(
        DiscardedHandleId,
        "Fitz lifecycle handle is discarded",
        "The {0} returned by '{1}' must be retained and disposed",
        "Reliability",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    internal static readonly DiagnosticDescriptor InvalidArgument = new(
        InvalidArgumentId,
        "Invalid Fitz operation argument",
        "Argument '{0}' has invalid constant value '{1}'",
        "Usage",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);
}
