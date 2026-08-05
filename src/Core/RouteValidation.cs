using System;
using System.Text;

namespace Cntryl.Fitz.Core;

internal enum RouteValidationFailure
{
    InvalidScheme,
    EmptySegment,
    InvalidShape,
    ContainsWildcard,
}

internal static class RouteValidation
{
    internal static bool IsConcreteRoute(string route, string scheme)
    {
        return TryValidateConcreteRoute(route, scheme, out _);
    }

    internal static bool IsFixedRoute(string route, string scheme, int segmentCount)
    {
        return TryValidateFixedRoute(route, scheme, segmentCount, out _);
    }

    internal static bool IsSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard)
    {
        return TryValidateSelectorRoute(route, scheme, segmentCount, allowRealmWildcard, out _);
    }

    internal static bool IsRegistrationPattern(string route, string scheme, int requiredSegments = 0)
    {
        return TryValidateRegistrationPattern(route, scheme, requiredSegments, out _);
    }

    internal static bool IsStreamSelector(string route)
    {
        if (string.Equals(route, "stream://**", StringComparison.Ordinal))
        {
            return true;
        }
        var segments = RouteSegments(route);
        if (segments.Length != 3 || !route.StartsWith("stream://", StringComparison.Ordinal))
        {
            return false;
        }
        return !segments[0].Contains('*', StringComparison.Ordinal) &&
            ((segments[1] != "*" && !segments[1].Contains('*', StringComparison.Ordinal) && (segments[2] == "*" || !segments[2].Contains('*', StringComparison.Ordinal))) ||
             (segments[1] == "*" && segments[2] == "*"));
    }

    internal static bool TryValidateRegistrationPattern(
        string route,
        string scheme,
        int requiredSegments,
        out RouteValidationFailure failure)
    {
        if (requiredSegments < 0 || !TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        var segmentCount = 0;
        var doubleWildcardCount = 0;
        var segmentStart = pathStart;
        for (var index = pathStart; index <= route.Length; index++)
        {
            if (index != route.Length && route[index] != '/')
            {
                continue;
            }

            var length = index - segmentStart;
            if (length == 0)
            {
                failure = RouteValidationFailure.EmptySegment;
                return false;
            }

            var single = IsSingleWildcard(route, segmentStart, length);
            var doubleWildcard = IsDoubleWildcard(route, segmentStart, length);
            if (!single && !doubleWildcard)
            {
                for (var cursor = segmentStart; cursor < index; cursor++)
                {
                    if (route[cursor] == '*')
                    {
                        failure = RouteValidationFailure.ContainsWildcard;
                        return false;
                    }
                }
            }
            if (doubleWildcard)
            {
                doubleWildcardCount++;
            }
            segmentCount++;
            segmentStart = index + 1;
        }

        var canMatchRequiredDepth = requiredSegments == 0 ||
            (doubleWildcardCount == 0
                ? segmentCount == requiredSegments
                : segmentCount - doubleWildcardCount <= requiredSegments);
        failure = canMatchRequiredDepth ? default : RouteValidationFailure.InvalidShape;
        return canMatchRequiredDepth;
    }

    internal static bool MatchesPattern(string route, string pattern)
    {
        var routeMarker = route.IndexOf("://", StringComparison.Ordinal);
        var patternMarker = pattern.IndexOf("://", StringComparison.Ordinal);
        if (routeMarker < 1 || patternMarker < 1 ||
            !route.AsSpan(0, routeMarker).SequenceEqual(pattern.AsSpan(0, patternMarker)))
        {
            return false;
        }
        var routeSegments = RouteSegments(route);
        var patternSegments = RouteSegments(pattern);
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

    private static string[] RouteSegments(string route)
    {
        var marker = route.IndexOf("://", StringComparison.Ordinal);
        return marker < 1 ? Array.Empty<string>() : route[(marker + 3)..].Split('/');
    }

    internal static bool TryValidateConcreteRoute(string route, string scheme, out RouteValidationFailure failure)
    {
        return TryScan(route, scheme, expectedSegments: 0, allowWildcards: false, allowRealmWildcard: false, out _, out failure);
    }

    internal static bool TryValidateFixedRoute(string route, string scheme, int segmentCount, out RouteValidationFailure failure)
    {
        return TryScan(route, scheme, segmentCount, allowWildcards: false, allowRealmWildcard: false, out _, out failure);
    }

    internal static bool TryValidateSelectorRoute(string route, string scheme, int segmentCount, bool allowRealmWildcard, out RouteValidationFailure failure)
    {
        return TryScan(route, scheme, segmentCount, allowWildcards: true, allowRealmWildcard, out _, out failure);
    }

    private static bool TryScan(
        string route,
        string scheme,
        int expectedSegments,
        bool allowWildcards,
        bool allowRealmWildcard,
        out int segmentCount,
        out RouteValidationFailure failure)
    {
        segmentCount = 0;
        if (!TryGetPathStart(route, scheme, out var pathStart, out failure))
        {
            return false;
        }

        var firstWildcard = -1;
        var wildcardSuffix = true;
        var segmentStart = pathStart;
        for (var index = pathStart; index <= route.Length; index++)
        {
            if (index != route.Length && route[index] != '/')
            {
                continue;
            }

            var length = index - segmentStart;
            if (length == 0)
            {
                failure = RouteValidationFailure.EmptySegment;
                return false;
            }

            var wildcard = IsWildcardSegment(route, segmentStart, length);
            if (wildcard)
            {
                if (!allowWildcards || IsDoubleWildcard(route, segmentStart, length))
                {
                    failure = RouteValidationFailure.ContainsWildcard;
                    return false;
                }
                firstWildcard = firstWildcard < 0 ? segmentCount : firstWildcard;
            }
            else
            {
                for (var cursor = segmentStart; cursor < index; cursor++)
                {
                    if (route[cursor] == '*')
                    {
                        failure = RouteValidationFailure.ContainsWildcard;
                        return false;
                    }
                }
                wildcardSuffix &= firstWildcard < 0;
            }

            segmentCount++;
            segmentStart = index + 1;
        }

        if (expectedSegments > 0 && segmentCount != expectedSegments)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }
        if (firstWildcard == 0 || !wildcardSuffix)
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }
        if (firstWildcard >= 0 && firstWildcard != segmentCount - 1 && !(allowRealmWildcard && firstWildcard == 1))
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        failure = default;
        return true;
    }

    private static bool TryGetPathStart(string route, string scheme, out int pathStart, out RouteValidationFailure failure)
    {
        pathStart = 0;
        failure = RouteValidationFailure.InvalidScheme;

        if (string.IsNullOrWhiteSpace(route) || string.IsNullOrWhiteSpace(scheme))
        {
            return false;
        }
        if (route.Length > ushort.MaxValue || (!IsAscii(route) && Encoding.UTF8.GetByteCount(route) > ushort.MaxValue))
        {
            failure = RouteValidationFailure.InvalidShape;
            return false;
        }

        var schemeLength = scheme.Length;
        if (route.Length <= schemeLength + 2)
        {
            return false;
        }

        if (!route.AsSpan(0, schemeLength).SequenceEqual(scheme))
        {
            return false;
        }

        if (route[schemeLength] != ':' || route[schemeLength + 1] != '/' || route[schemeLength + 2] != '/')
        {
            return false;
        }

        pathStart = schemeLength + 3;
        if (route.Length <= pathStart)
        {
            failure = RouteValidationFailure.EmptySegment;
            return false;
        }

        return true;
    }

    private static bool IsConcreteSegment(string route, int start, int length)
    {
        return !IsWildcardSegment(route, start, length);
    }

    private static bool IsAscii(string value)
    {
        foreach (var character in value)
        {
            if (character > 0x7f)
            {
                return false;
            }
        }
        return true;
    }

    private static bool IsWildcardSegment(string route, int start, int length)
    {
        return IsSingleWildcard(route, start, length) || IsDoubleWildcard(route, start, length);
    }

    private static bool IsSingleWildcard(string route, int start, int length)
    {
        return length == 1 && route[start] == '*';
    }

    private static bool IsDoubleWildcard(string route, int start, int length)
    {
        return length == 2 && route[start] == '*' && route[start + 1] == '*';
    }
}
