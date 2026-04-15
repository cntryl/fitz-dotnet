namespace Cntryl.Fitz.Runtime;

internal static class RoutePatternMatcher
{
    internal static bool Matches(string route, string pattern)
    {
        var routeSegments = route.Split('/', StringSplitOptions.None);
        var patternSegments = pattern.Split('/', StringSplitOptions.None);

        var routeIndex = 0;
        var patternIndex = 0;

        while (patternIndex < patternSegments.Length && routeIndex < routeSegments.Length)
        {
            var segment = patternSegments[patternIndex];
            if (segment == "**")
            {
                return true;
            }

            if (segment != "*" && !string.Equals(segment, routeSegments[routeIndex], StringComparison.Ordinal))
            {
                return false;
            }

            patternIndex++;
            routeIndex++;
        }

        if (patternIndex == patternSegments.Length && routeIndex == routeSegments.Length)
        {
            return true;
        }

        return patternIndex == patternSegments.Length - 1 && patternSegments[patternIndex] == "**";
    }
}