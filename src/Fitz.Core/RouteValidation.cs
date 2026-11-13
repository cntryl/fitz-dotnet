using System;

namespace Cntryl.Fitz;

static class RouteValidation
{
    internal static bool IsConcreteRoute(string route, string scheme) => TryValidateConcreteRoute(route, scheme, out _);

    internal static bool IsFixedRoute(string route, string scheme, int segmentCount) => TryValidateFixedRoute(route, scheme, segmentCount, out _);

    internal static bool IsSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard) => TryValidateSelectorRoute(route, scheme, segmentCount, allowRealmWildcard, out _);

    internal static bool IsRegistrationPattern(string route, string scheme, int requiredSegments = 0) => TryValidateRegistrationPattern(route, scheme, requiredSegments, out _);

    internal static bool IsStreamSelector(string route) => TryGetStreamSelectorScope(route, out _);

    internal static bool TryGetStreamSelectorScope(string route, out StreamSelectorScope scope) => RouteGrammar.TryGetStreamSelectorScope(route, out scope);

    internal static bool TryValidateRegistrationPattern(
        string route,
        string scheme,
        int requiredSegments,
        out RouteValidationFailure failure) => RouteGrammar.TryValidateRegistrationPattern(route, scheme, requiredSegments, out failure);

    internal static bool MatchesPattern(string route, string pattern)
    {
        var routeMarker = route.IndexOf("://", StringComparison.Ordinal);
        var patternMarker = pattern.IndexOf("://", StringComparison.Ordinal);
        if (routeMarker < 1 || patternMarker < 1 ||
            !route.AsSpan(0, routeMarker).SequenceEqual(pattern.AsSpan(0, patternMarker)))
        {
            return false;
        }
        var routeSegments = RouteGrammar.Segments(route);
        var patternSegments = RouteGrammar.Segments(pattern);
        if (routeSegments.Length == 0 || patternSegments.Length == 0)
        {
            return false;
        }

        var routeIndex = 0;
        var patternIndex = 0;
        var lastDoubleWildcard = -1;
        var lastDoubleMatch = 0;
        while (routeIndex < routeSegments.Length)
        {
            var segment = patternIndex < patternSegments.Length ? patternSegments[patternIndex] : null;
            if (segment == "*" || string.Equals(segment, routeSegments[routeIndex], StringComparison.Ordinal))
            {
                routeIndex++;
                patternIndex++;
                continue;
            }
            if (segment == "**")
            {
                lastDoubleWildcard = patternIndex++;
                lastDoubleMatch = routeIndex;
                continue;
            }
            if (lastDoubleWildcard < 0)
            {
                return false;
            }
            routeIndex = ++lastDoubleMatch;
            patternIndex = lastDoubleWildcard + 1;
        }
        while (patternIndex < patternSegments.Length && patternSegments[patternIndex] == "**")
        {
            patternIndex++;
        }
        return patternIndex == patternSegments.Length;
    }

    internal static bool TryValidateConcreteRoute(string route, string scheme, out RouteValidationFailure failure) => RouteGrammar.TryValidateConcreteRoute(route, scheme, out failure);

    internal static bool TryValidateFixedRoute(string route, string scheme, int segmentCount, out RouteValidationFailure failure) => RouteGrammar.TryValidateFixedRoute(route, scheme, segmentCount, out failure);

    internal static bool TryValidateSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard, out RouteValidationFailure failure) => RouteGrammar.TryValidateSelectorRoute(route, scheme, segmentCount, allowRealmWildcard, out failure);
}
