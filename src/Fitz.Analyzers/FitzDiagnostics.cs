using Microsoft.CodeAnalysis;

namespace Cntryl.Fitz.Analyzers;

static class FitzDiagnostics
{
    internal const string InvalidRouteId = "FITZ001";
    internal const string InvalidPatternId = "FITZ002";
    internal const string DiscardedHandleId = "FITZ003";
    internal const string InvalidArgumentId = "FITZ004";

    internal const string UsageCategory = "Usage";
    internal const string ReliabilityCategory = "Reliability";

    const string HelpLinkBase = "https://github.com/cntryl/fitz-dotnet/blob/main/docs/analyzers.md#";

    internal static readonly DiagnosticDescriptor InvalidRoute = new(
        InvalidRouteId,
        "Invalid Fitz route",
        "'{0}' is not a valid {1} route",
        UsageCategory,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A route passed to a Fitz operation must use the operation's scheme, have the segment count the scheme requires, contain no empty segments, and contain no wildcards. The client rejects any other route at runtime.",
        helpLinkUri: HelpLinkBase + "fitz001");

    internal static readonly DiagnosticDescriptor InvalidPattern = new(
        InvalidPatternId,
        "Invalid Fitz pattern",
        "'{0}' is not a valid {1} pattern or selector",
        UsageCategory,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A pattern or selector passed to a Fitz subscription, listing, or registration may use '*' for one whole segment and '**' for any number of whole segments, but nothing else, and it must be able to match the scheme's segment count. The client rejects any other pattern at runtime.",
        helpLinkUri: HelpLinkBase + "fitz002");

    internal static readonly DiagnosticDescriptor DiscardedHandle = new(
        DiscardedHandleId,
        "Fitz lifecycle handle is discarded",
        "The {0} returned by '{1}' must be retained and disposed",
        ReliabilityCategory,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Leases, transactions, subscriptions, worker registrations, and stream sessions own broker-side state that is released only when the handle is disposed. Discarding the handle leaks that state until the connection closes.",
        helpLinkUri: HelpLinkBase + "fitz003");

    internal static readonly DiagnosticDescriptor InvalidArgument = new(
        InvalidArgumentId,
        "Invalid Fitz operation argument",
        "Argument '{0}' has invalid constant value '{1}'",
        UsageCategory,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A constant batch size, limit, or duration passed to a Fitz operation is outside the range the operation accepts. Durations must be whole seconds, and TTLs and leases must be positive. The client rejects any other value at runtime.",
        helpLinkUri: HelpLinkBase + "fitz004");
}
